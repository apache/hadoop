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

"""Compare KMS HTTP throughput using two already-built hadoop-common jars.

No source files, installed artifacts, batch sizes, or storage delays are changed.
Only the child-server classpaths differ. All requests use the same HTTP harness.
"""

import argparse
import hashlib
import json
from pathlib import Path
import statistics
import subprocess
import time
import zipfile


CLASSES = [
    "org/apache/hadoop/security/token/delegation/"
    "AbstractDelegationTokenSecretManager.class",
    "org/apache/hadoop/security/token/delegation/"
    "ZKDelegationTokenSecretManager.class",
]


def class_hashes(jar):
    with zipfile.ZipFile(jar) as archive:
        return [hashlib.sha256(archive.read(name)).hexdigest() for name in CLASSES]


def write_json(path, value):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def summarize(samples):
    summary = []
    for percent in [0, 1, 5, 10]:
        row = {"verifyPercent": 100 - percent, "createPercent": percent}
        for label in ["baseline", "fixed"]:
            runs = [s for s in samples
                    if s["label"] == label and s["createPercent"] == percent]
            if not runs:
                continue
            rates = [s["successfulRequestsPerSecond"] for s in runs]
            row[label] = {
                "runs": len(runs),
                "medianRequestsPerSecond": statistics.median(rates),
                "minRequestsPerSecond": min(rates),
                "maxRequestsPerSecond": max(rates),
                "medianVerifyPerSecond": statistics.median(
                    s["verify"]["successfulRequestsPerSecond"] for s in runs),
                "medianCreatePerSecond": statistics.median(
                    s["create"]["successfulRequestsPerSecond"] for s in runs),
                "medianVerifyP99Millis": statistics.median(
                    s["verify"]["p99Millis"] for s in runs),
                "medianCreateP99Millis": statistics.median(
                    s["create"]["p99Millis"] for s in runs),
                "errors": sum(s["verify"]["errors"] + s["create"]["errors"]
                              for s in runs),
            }
        if "baseline" in row and "fixed" in row:
            row["throughputRatio"] = (row["fixed"]["medianRequestsPerSecond"]
                                      / row["baseline"]["medianRequestsPerSecond"])
        summary.append(row)
    return summary


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
    parser.add_argument("--output", type=Path, default=(root / "hadoop-common-project"
                        / "hadoop-kms/target/kms-http-comparison"))
    args = parser.parse_args()
    if min(args.repetitions, args.concurrency, args.warmup_seconds, args.seconds) <= 0:
        parser.error("Repetitions, concurrency and durations must be positive")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    if (output / "comparison.json").exists():
        parser.error("Output already contains a comparison; choose a fresh directory")
    jars = {"baseline": args.baseline_common.resolve(),
            "fixed": args.fixed_common.resolve()}
    fingerprints = {label: class_hashes(jar) for label, jar in jars.items()}
    if fingerprints["baseline"] == fingerprints["fixed"]:
        parser.error("The two jars contain identical secret-manager classes")
    report = {
        "concurrency": args.concurrency, "sequenceBatchSize": 100,
        "addedDelayMillis": 0, "warmupSeconds": args.warmup_seconds,
        "measurementSeconds": args.seconds, "repetitions": args.repetitions,
        "commonJars": {label: str(jar) for label, jar in jars.items()},
        "classHashes": fingerprints, "samples": [], "summary": [],
    }
    for repetition in range(1, args.repetitions + 1):
        order = ["baseline", "fixed"] if repetition % 2 else ["fixed", "baseline"]
        for label in order:
            run = output / (label + "-r" + str(repetition))
            run.mkdir()
            command = [args.maven, "-o", "-pl", "hadoop-common-project/hadoop-kms",
                       "-Dtest=TestKMSHttpBenchmark", "-Dkms.benchmark=true",
                       "-Dkms.benchmark.repetitions=1",
                       "-Dkms.benchmark.concurrency=" + str(args.concurrency),
                       "-Dkms.benchmark.create.percent=0,1,5,10",
                       "-Dkms.benchmark.warmup.seconds=" + str(args.warmup_seconds),
                       "-Dkms.benchmark.seconds=" + str(args.seconds),
                       "-Dkms.benchmark.label=" + label,
                       "-Dkms.benchmark.common.jar=" + str(jars[label]),
                       "-Dkms.benchmark.output=" + str(run),
                       "-Dmaven.test.failure.ignore=false", "-Dcheckstyle.skip",
                       "-Dspotbugs.skip", "test"]
            write_json(run / "command.json", command)
            print("Starting " + label + " repetition " + str(repetition), flush=True)
            started = time.monotonic()
            with (run / "maven.log").open("w") as log:
                subprocess.run(command, cwd=root, stdout=log, stderr=subprocess.STDOUT,
                               timeout=4 * (args.warmup_seconds + args.seconds + 180),
                               check=True)
            samples = json.loads((run / "results.json").read_text())
            if len(samples) != 4 or {s["createPercent"] for s in samples} != {0, 1, 5, 10}:
                raise RuntimeError("Missing request mix in " + str(run))
            for sample in samples:
                observed = [sample["serverMetadata"]["abstractManagerSha256"],
                            sample["serverMetadata"]["zkManagerSha256"]]
                if not sample["valid"] or observed != fingerprints[label]:
                    raise RuntimeError("Invalid sample or wrong loaded classes: " + str(run))
                sample["repetition"] = repetition
                report["samples"].append(sample)
            report["summary"] = summarize(report["samples"])
            write_json(output / "comparison.json", report)
            print("Completed " + label + " repetition " + str(repetition)
                  + " in %.1fs" % (time.monotonic() - started), flush=True)
    print(json.dumps(report["summary"], indent=2))
    print("Saved " + str(output / "comparison.json"))


if __name__ == "__main__":
    main()
