using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Xml;
using System.Threading;

namespace HadoopServiceHost
{
  // Simple service host that allows running of Hadoop services in the context
  // of Windows services. The service host is configured through a simple
  // xml file named after the exe.
  // Example xml:
  //   <service>
  //     <name>datanode</name>
  //     <executable>I:\git\tools\java\bin\java</executable>
  //     <arguments>-server org.apache.hadoop.hdfs.server.datanode.DataNode</arguments>
  //   </service>
  public partial class ServiceHost : ServiceBase
  {
    private String serviceExe;
    private String serviceArgs;
    private volatile bool inShutdown = false;
    Process process;
    TraceSource trace = new TraceSource("HadoopServiceTraceSource");

    private void InitInternal(String fileName)
    {
      try
      {
        XmlDocument doc = new XmlDocument();
        trace.TraceEvent(TraceEventType.Information, 0,
          "Loading service xml: " + fileName);
        doc.Load(fileName);

        this.ServiceName = doc.SelectSingleNode("//service/name").InnerText;
        this.CanShutdown = true;
        this.CanStop = true;
        this.CanPauseAndContinue = false;

        this.serviceExe = doc.SelectSingleNode("//service/executable").InnerText;
        this.serviceArgs = doc.SelectSingleNode("//service/arguments").InnerText;
        if (ServiceName == null || serviceExe == null || serviceArgs == null)
        {
          trace.TraceEvent(TraceEventType.Error, 0,
            "Invalid service XML file format");
          throw new Exception("Invalid service XML file format");
        }
        trace.TraceEvent(TraceEventType.Information, 0,
          "Successfully parsed service xml for service " + ServiceName);
        trace.TraceEvent(TraceEventType.Information, 0, "Command line: "
          + serviceExe + " " + serviceArgs);
      }
      catch (Exception ex)
      {
        trace.TraceEvent(TraceEventType.Error, 0,
          "Failed to parse the service xml with exception" + ex);
        throw;
      }
    }

    private void TerminateOurselves(int exitCode)
    {
      trace.Flush();
      System.Environment.Exit(exitCode);
    }

    private void InitTracing()
    {
      TextWriterTraceListener tr = new TextWriterTraceListener(
        System.Reflection.Assembly.GetExecutingAssembly().Location.Replace(".exe",
        ".trace.log"));
      trace.Switch = new SourceSwitch("Switch", "switch");
      trace.Switch.Level = SourceLevels.Information;
      tr.TraceOutputOptions = TraceOptions.DateTime | TraceOptions.Timestamp;
      trace.Listeners.Clear();
      trace.Listeners.Add(tr);
      Trace.AutoFlush = true;

      trace.TraceEvent(TraceEventType.Information, 0,
        "Tracing successfully initialized");
    }

    public ServiceHost()
    {
      InitTracing();
      String xmlFileName =
        System.Reflection.Assembly.GetExecutingAssembly().Location.Replace(".exe",
        ".xml");
      InitInternal(xmlFileName);
      process = new Process();
    }

    protected override void OnStart(string[] args)
    {
      trace.TraceEvent(TraceEventType.Information, 0, "ServiceHost#OnStart");

      process.StartInfo.FileName = this.serviceExe;
      process.StartInfo.Arguments = this.serviceArgs;
      process.StartInfo.WorkingDirectory = AppDomain.CurrentDomain.BaseDirectory;
      process.StartInfo.CreateNoWindow = false;
      process.StartInfo.UseShellExecute = false;

      if (!process.Start())
      {
        trace.TraceEvent(TraceEventType.Error, 0,
          "Process#Start failed, terminating service host");
        TerminateOurselves(1);
      }
      else
      {
        trace.TraceEvent(TraceEventType.Information, 0,
          "Child process started, PID: " + process.Id);

        // Start a separate thread that will wait for child process to exit
        new Thread(delegate()
        {
          process.WaitForExit();
          trace.TraceEvent(TraceEventType.Information, 0,
            "Child process exited with exit code: " + process.ExitCode);

          if (!inShutdown)
          {
            // If not in shutdown state, child process terminated outside of
            // our control, we should also stop ourselves
            trace.TraceEvent(TraceEventType.Information, 0,
              "Service host not in shutdown mode, terminating service host");
            TerminateOurselves(process.ExitCode);
          }
          // else, we should be stopped anyways

        }).Start();
      }
    }

    protected override void OnStop()
    {
      trace.TraceEvent(TraceEventType.Information, 0,
        "ServiceHost#OnStop, killing process " + process.Id);
      inShutdown = true;
      process.Kill();
    }

    protected override void OnShutdown()
    {
      trace.TraceEvent(TraceEventType.Information, 0,
        "ServiceHost#OnShutdown, killing process " + process.Id);
      inShutdown = true;
      process.Kill();
    }
  }
}
