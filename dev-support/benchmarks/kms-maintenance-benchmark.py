#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Compare verification traffic during real KMS secret-manager maintenance.

Fresh KMS and ZooKeeper JVMs per case, fixed background work, no injected delay.
"""
import argparse
import importlib.util
import json
from pathlib import Path
import statistics
import subprocess

spec = importlib.util.spec_from_file_location(
    "kms_http_comparison", Path(__file__).with_name("kms-http-benchmark.py"))
common = importlib.util.module_from_spec(spec)
spec.loader.exec_module(common)
WORK = {"new-key": 500, "rotate": 500, "expired-keys": 1000, "expired-tokens": 5000}


def summarize(samples):
    result = []
    for operation in WORK:
        row = {"maintenanceOperation": operation, "work": WORK[operation]}
        for label in ("baseline", "fixed"):
            runs = [s for s in samples if s["label"] == label
                    and s["maintenanceOperation"] == operation]
            if not runs:
                continue
            def values(path):
                numbers = []
                for sample in runs:
                    value = sample
                    for part in path.split("."):
                        value = value[part]
                    numbers.append(value)
                return {"median": statistics.median(numbers),
                        "min": min(numbers), "max": max(numbers)}
            row[label] = {
                "runs": len(runs),
                "activeVerifyRequestsPerSecond": values(
                    "maintenanceImpact.verifyRequestsPerSecondDuringMaintenance"),
                "maintenanceElapsedNanos": values("maintenance.elapsedNanos"),
                "overlapVerifyP99Millis": values(
                    "maintenanceImpact.overlappingVerificationLatency.p99Millis"),
                "overlapVerifyP9999Millis": values(
                    "maintenanceImpact.overlappingVerificationLatency.p9999Millis"),
                "overlapVerifyMaxMillis": values(
                    "maintenanceImpact.overlappingVerificationLatency.maxMillis"),
                "wholeWindowVerifyRequestsPerSecond": values("successfulRequestsPerSecond"),
                "warmupVerifyRequestsPerSecond": values("warmupVerify.successfulRequestsPerSecond"),
                "errors": sum(s["verify"]["errors"] for s in runs),
            }
        result.append(row)
    return result


def main():
    root = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-common", type=Path, required=True)
    parser.add_argument("--fixed-common", type=Path, required=True)
    parser.add_argument("--maven", default="mvn")
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--concurrency", type=int, default=32)
    parser.add_argument("--warmup-seconds", type=int, default=5)
    parser.add_argument("--seconds", type=int, default=15)
    parser.add_argument("--operations", default=",".join(WORK))
    parser.add_argument("--resume", action="store_true", help="Resume validated cases with matching settings")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    operations = args.operations.split(",")
    if any(op not in WORK for op in operations) or len(set(operations)) != len(operations):
        parser.error("Operations must be known and unique")
    if min(args.repetitions, args.concurrency, args.warmup_seconds, args.seconds) <= 0:
        parser.error("Counts and durations must be positive")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()) and not args.resume:
        parser.error("Output directory must be empty; use --resume for a matching comparison")
    jars = {"baseline": args.baseline_common.resolve(), "fixed": args.fixed_common.resolve()}
    hashes = {label: common.class_hashes(jar) for label, jar in jars.items()}
    if hashes["baseline"] == hashes["fixed"]:
        parser.error("Secret-manager classes must differ")
    report = {"concurrency": args.concurrency, "sequenceBatchSize": 100,
              "addedDelayMillis": 0, "work": WORK, "repetitions": args.repetitions,
              "warmupSeconds": args.warmup_seconds, "measurementSeconds": args.seconds,
              "classHashes": hashes, "samples": [], "summary": []}
    durations = {op: max(args.seconds, 60) if op == "expired-tokens" else args.seconds
                 for op in WORK}
    if args.resume:
        previous = json.loads((output / "comparison.json").read_text())
        for key in ("concurrency", "sequenceBatchSize", "addedDelayMillis", "work",
                    "repetitions", "warmupSeconds", "measurementSeconds", "classHashes"):
            if previous[key] != report[key]:
                parser.error("Cannot resume with changed " + key)
        for sample in previous["samples"]:
            if sample["measurementSeconds"] != durations[sample["maintenanceOperation"]]:
                parser.error("Existing sample has a different measurement duration")
        report = previous
    report["measurementSecondsByOperation"] = durations
    completed = {(s["maintenanceOperation"], s["label"], s["repetition"])
                 for s in report["samples"]}
    for repetition in range(1, args.repetitions + 1):
        for index, operation in enumerate(operations):
            order = ["baseline", "fixed"] if (repetition + index) % 2 else ["fixed", "baseline"]
            for label in order:
                if (operation, label, repetition) in completed:
                    continue
                name = operation + "-" + label + "-r" + str(repetition)
                run = output / name
                attempt = 1
                while run.exists():
                    run = output / (name + "-retry" + str(attempt))
                    attempt += 1
                run.mkdir()
                command = [args.maven, "-o", "-pl", "hadoop-common-project/hadoop-kms",
                           "-Dtest=TestKMSHttpBenchmark", "-Dkms.benchmark=true",
                           "-Dkms.benchmark.repetitions=1", "-Dkms.benchmark.create.percent=0",
                           "-Dkms.benchmark.maintenance=" + operation,
                           "-Dkms.benchmark.maintenance.work=" + str(WORK[operation]),
                           "-Dkms.benchmark.concurrency=" + str(args.concurrency),
                           "-Dkms.benchmark.warmup.seconds=" + str(args.warmup_seconds),
                           "-Dkms.benchmark.seconds=" + str(durations[operation]),
                           "-Dkms.benchmark.common.jar=" + str(jars[label]),
                           "-Dkms.benchmark.label=" + label, "-Dkms.benchmark.output=" + str(run),
                           "-Dmaven.test.failure.ignore=false", "-Dcheckstyle.skip",
                           "-Dspotbugs.skip", "test"]
                common.write_json(run / "command.json", command)
                print("Starting", run.name, flush=True)
                with (run / "maven.log").open("w") as log:
                    subprocess.run(command, cwd=root, stdout=log, stderr=subprocess.STDOUT,
                                   check=True, timeout=600)
                cases = json.loads((run / "results.json").read_text())
                if len(cases) != 1:
                    raise RuntimeError("Expected one maintenance case")
                sample = cases[0]
                observed = [sample["serverMetadata"]["abstractManagerSha256"],
                            sample["serverMetadata"]["zkManagerSha256"]]
                if not sample["valid"] or observed != hashes[label]:
                    raise RuntimeError("Invalid run or wrong loaded artifact")
                if sample["maintenanceOperation"] != operation:
                    raise RuntimeError("Wrong maintenance operation")
                if sample["maintenance"]["work"] != WORK[operation]:
                    raise RuntimeError("Wrong maintenance work size")
                sample["repetition"] = repetition
                report["samples"].append(sample)
                report["summary"] = summarize(report["samples"])
                common.write_json(output / "comparison.json", report)
                print("Completed", run.name, "maintenance ms:",
                      round(sample["maintenance"]["elapsedNanos"] / 1e6, 2), flush=True)
    print(json.dumps(report["summary"], indent=2))


if __name__ == "__main__":
    main()
