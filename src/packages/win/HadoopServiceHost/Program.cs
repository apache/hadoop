using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceProcess;
using System.Text;

namespace HadoopServiceHost
{
  static class Program
  {
    /// <summary>
    /// The main entry point for the application.
    /// </summary>
    static void Main()
    {
      ServiceBase[] ServicesToRun;
      ServicesToRun = new ServiceBase[] 
      {
        new ServiceHost() 
      };
      ServiceBase.Run(ServicesToRun);
    }
  }
}
