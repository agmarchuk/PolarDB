using Factograph.Data;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Factograph.Data
{
    public class RRecord
    {
        public string Id { get; private set; } = string.Empty;
        public string Tp { get; private set; } = string.Empty;
        public RProperty[] Props { get; private set; } = Array.Empty<RProperty>();
        private IFDataService db;
        public RRecord(string id, string tp, RProperty[] props, IFDataService db)
        {
            Id = id;
            Tp = tp;
            Props = props;
            this.db = db;
        }

        public string? GetField(string propName)
        {
            RProperty? query = Props.FirstOrDefault(p => p is RField && p.Prop == propName);
            return query == null ? null : ((RField)query).Value;
        }
        public string? GetDirectResource(string propName)
        {
            var prop = this.Props.FirstOrDefault(p => p.Prop == propName);
            if (prop == null) return null;
            if (prop is RLink) return ((RLink)prop).Resource;
            return null;
        }
        public RRecord? GetDirect(string propName)
        {
            if (propName == null) return null;
            var prop = this.Props.FirstOrDefault(p => p is RLink && p.Prop == propName);
            if (prop == null) return null;
            string resource = ((RLink)prop).Resource;
            return db.GetRRecord(resource, false);
        }

        public string GetName()
        {
            return ((RField)this.Props.FirstOrDefault(p => p is RField && p.Prop == "http://fogid.net/o/name"))?.Value;
        }
        public string GetName(string lang)
        {
            var name = ((RField)this.Props.FirstOrDefault(p => p is RField && ((RField)p).Lang == lang && p.Prop == "http://fogid.net/o/name"));
            if (name != null)
            {
                return name.Value;
            }
            else
            {
                name = ((RField)this.Props.FirstOrDefault(p => p is RField && p.Prop == "http://fogid.net/o/name"));
                if (name == null) return null;
                var langName = (name.Lang == null) ? "ru" : name.Lang;
                if (langName != lang)
                {
                    return name.Value + " (" + langName + ")";
                }
                else
                {
                    return name.Value;
                }
            }
        }
        public string GetDates()
        {
            string df = GetField("http://fogid.net/o/from-date");
            string dt = GetField("http://fogid.net/o/to-date");
            return (df == null ? "" : df) + (string.IsNullOrEmpty(dt) ? "" : "-" + dt);
        }
    }
    public abstract class RProperty
    {
        public string Prop { get; set; }
    }
    public class RField : RProperty
    {
        public string Value { get; set; }
        public string Lang { get; set; }
    }
    public class RLink : RProperty, IEquatable<RLink>
    {
        public string Resource { get; set; }

        public bool Equals(RLink other)
        {
            return this.Prop == other.Prop && this.Resource == other.Resource;
        }

        public int GetHashCode([DisallowNull] RLink obj)
        {
            return obj.Prop.GetHashCode() ^ obj.Resource.GetHashCode();
        }
    }
    // Расширение вводится на странице 11 пособия "Делаем фактографию"
    public class RInverseLink : RProperty
    {
        public string Source { get; set; }
    }


    // Custom comparer for the RRecord class
    public class RRecordComparer : IEqualityComparer<RRecord>
    {
        public bool Equals(RRecord x, RRecord y)
        {
            //Check whether the compared objects reference the same data.
            if (Object.ReferenceEquals(x, y)) return true;

            //Check whether any of the compared objects is null.
            if (Object.ReferenceEquals(x, null) || Object.ReferenceEquals(y, null))
                return false;
            return x.Id == y.Id;
        }

        // If Equals() returns true for a pair of objects
        // then GetHashCode() must return the same value for these objects.

        public int GetHashCode([DisallowNull] RRecord obj)
        {
            //Check whether the object is null
            if (Object.ReferenceEquals(obj, null)) return 0;
            return obj.Id.GetHashCode();
        }
    }

    // Новое расширение
    public class RDirect : RProperty
    {
        public RRecord DRec { get; set; }
    }
    //public class RInverse : RProperty
    //{
    //    public RRecord IRec { get; set; }
    //}
    //// Еще более новое расширение
    //public class RMultiInverse : RProperty
    //{
    //    public RRecord[] IRecs { get; set; }
    //}

    // Специальное расширение для описателей перечислимых  
    public class RState : RProperty
    {
        public string Key { get; set; }
        public string Value { get; set; }
        public string lang { get; set; }
    }





}
