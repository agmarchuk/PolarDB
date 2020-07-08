using Polar.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Polar.Samples.Test1
{
    public class Sample5 :ISample
    {
        public ICollection<IField> Fields { get; set; }

        public void Run()
        {

        }
        public string Name { get; set; }
        public string DiplayName { get => "Sample5"; }
    }
}
