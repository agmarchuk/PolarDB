using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Polar.DB;

namespace Polar.OModel
{
    public class UniversalIndex
    {
        //private UniversalSequence sequence;
        //private IndexKey32CompImmutable index;
        private readonly IBearing sequence;
        private IIndexImmutable index;
        Func<object, int> hashFunc;
        Comparer<object> comp;
        public UniversalIndex(Func<Stream> streamGen, UniversalSequence sequence, Func<object, int> hashFunc, Comparer<object> comp)
        {
            this.hashFunc = hashFunc;
            this.comp = comp;
            if (hashFunc != null)
            {
                if (comp != null)
                {
                    index = new IndexHash32CompImmutable(streamGen, sequence, obj => true, hashFunc, comp);
                }
                else
                {
                    index = new IndexKey32CompImmutable(streamGen, ((UniversalSequence)sequence).usb, obj => true, hashFunc, comp);
                }
            }
            else
            {
                index = new IndexViewImmutable(streamGen, sequence, comp, @"D:\Data\tmp\", 1000_000);
            }
        }
        public void Clear() { index.Clear(); }
        public void Build()
        {
            index.Build();
        }
        public void Refresh()
        {
            index.Refresh();
        }

        public IEnumerable<object> GetAllBySample(object sample)
        {
            if (index is IndexKey32CompImmutable)
            {
                var query = ((IndexKey32CompImmutable)index).GetAllBySample(sample).ToArray();
                return query;
            }
            else if (index is IndexHash32CompImmutable)
            {
                var query = ((IndexHash32CompImmutable)index).GetAllBySample(sample).ToArray();
                return query;
            }
            else if (index is IndexViewImmutable)
            {
                var query = ((IndexViewImmutable)index).SearchAll(sample);
                return query;
            }
            else throw new Exception("Err in GetBySample");
        }
        public object GetBySample(object sample)
        {
            if (index is IndexHash32CompImmutable)
            {
                return ((IndexHash32CompImmutable)index).GetBySample(sample);
            }
            else if (index is IndexKey32CompImmutable)
            {
                return ((IndexKey32CompImmutable)index).GetAllBySample(sample).FirstOrDefault();
            }
            else throw new NotImplementedException("Err: not implemented for " + index.GetType().Name);
        }
        public IEnumerable<object> Like(object sample, Comparer<object> like_comp)
        {
            if (index is IndexViewImmutable)
            {
                return ((IndexViewImmutable)index).SearchAll(sample, like_comp);
            }
            throw new Exception("Err in Like");
        }
    }
    public class HashComp
    {
        public Func<object, int> Hash { get; set; }
        public Comparer<object> Comp { get; set; }
    }


    class Addition
    {
        private void Mixture()
        {
            // Это компаратор сортировки. (более ранний комментарий: компаратор надо поменять!!!!!!)
            Comparer<object> comp = Comparer<object>.Create(new Comparison<object>((object a, object b) =>
            {
                object predval1 = ((object[])((object[])a)[2]).FirstOrDefault(pair => (int)((object[])pair)[0] == 777); //cod_name
                object predval2 = ((object[])((object[])b)[2]).FirstOrDefault(pair => (int)((object[])pair)[0] == 777);

                return string.Compare(
                    (string)((object[])predval1)[1],
                    (string)((object[])predval2)[1],
                    StringComparison.OrdinalIgnoreCase);
            }));

            Comparer<object> comp_like = Comparer<object>.Create(new Comparison<object>((object a, object b) =>
            {
                string val1 = (string)((object[])((object[])((object[])a)[2]).FirstOrDefault(pair => (int)((object[])pair)[0] == 777))[1];
                string val2 = (string)((object[])((object[])((object[])b)[2]).FirstOrDefault(pair => (int)((object[])pair)[0] == 777))[1];
                if (string.IsNullOrEmpty(val2)) return 0;
                int len = val2.Length;
                return string.Compare(
                    val1, 0,
                    val2, 0, len, StringComparison.OrdinalIgnoreCase);
            }));

        }

    }
}

