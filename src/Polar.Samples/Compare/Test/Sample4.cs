using Polar.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Polar.Samples.Compare.Test
{
    public class Sample4 : ISample
    {
        public ICollection<IField> Fields { get; set; }

        public void Run()
        {

        }
        public string Name { get; set; }
        public string DiplayName { get => "Sample4"; }
    }
}
