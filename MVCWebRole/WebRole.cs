using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Diagnostics;
using Microsoft.WindowsAzure.ServiceRuntime;

namespace MVCWebRole
{
    public class WebRole : RoleEntryPoint
    {
        public override bool OnStart()
        {
            // For information on handling configuration changes
            // see the MSDN topic at https://go.microsoft.com/fwlink/?LinkId=166357.
            RoleEnvironment.TraceSource.Switch.Level = SourceLevels.Information;
            return base.OnStart();
        }
    }
}
