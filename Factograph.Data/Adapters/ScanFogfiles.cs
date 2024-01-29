using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Xml;
using System.Xml.Linq;

namespace Factograph.Data.Adapters
{
    class ScanFogfiles
    {
        //private Stream stream;
        private string[] fogfile_names;
        private Func<XElement, XElement> transformXElement = null;
        private Func<XElement, bool> useXElement = null; // true - OK, false - break
        public ScanFogfiles(string[] fogfile_names, 
            Func<XElement, XElement> transformXElement,
            Func<XElement, bool> useXElement)
        {
            //this.stream = stream;
            this.fogfile_names = fogfile_names;
            this.transformXElement = transformXElement;
            this.useXElement = useXElement;
        }
        public void Scan()
        {
            foreach (string ffname in fogfile_names)
            {
            Stream stream = File.OpenRead(ffname);
            XmlReaderSettings settings = new XmlReaderSettings();
            settings.Async = false;

            using (XmlReader reader = XmlReader.Create(stream, settings))
            {
                XElement xrecord = null;
                XElement xelement = null;
                int level = 0; // Увеличивается при заходе в элемент и уменьшается при выходе.
                               // уровень сканирования: 0 - начало, 1 - rdf:RDF, 2 - записи, 3 - поля записей
                while (reader.Read())
                {
                    switch (reader.NodeType)
                    {
                        case XmlNodeType.Element:
                            level++;
                            if (level == 2) xrecord = new XElement(XName.Get(reader.LocalName, reader.NamespaceURI));
                            if (level == 3) xelement = new XElement(XName.Get(reader.LocalName, reader.NamespaceURI));
                            if (reader.HasAttributes)
                            {
                                for (int i = 0; i < reader.AttributeCount; i++)
                                {
                                    reader.MoveToAttribute(i);
                                    if (level == 2) xrecord.Add(new XAttribute(XName.Get(reader.LocalName, reader.NamespaceURI), reader.Value));
                                    if (level == 3) xelement.Add(new XAttribute(XName.Get(reader.LocalName, reader.NamespaceURI), reader.Value));
                                }
                                reader.MoveToElement(); // Moves the reader back to the element node.
                            }
                            if (reader.IsEmptyElement)
                            {
                                if (level == 3) { xrecord.Add(new XElement(xelement)); }
                                else if (level == 2)
                                {
                                    // Странная запись. Возможно substitute Рабочая зона
                                    throw new Exception("========= Strange ========" + xrecord);
                                }
                                level--;
                            }
                            break;
                        case XmlNodeType.Text:
                            if (level == 2)
                            {
                                xrecord.Add(reader.Value);
                            }
                            else if (level == 3)
                            {
                                xelement.Add(reader.Value);
                            }
                            break;
                        case XmlNodeType.EndElement:
                            string name = reader.Name;
                            if (level == 3) { xrecord.Add(new XElement(xelement)); }
                            else if (level == 2)
                            {
                                // Рабочая зона
                                var xx = transformXElement(xrecord);
                                if (xx != null) { bool ok = useXElement(xx); if (!ok) break; }
                            }
                            level--;
                            break;
                        case XmlNodeType.Whitespace:
                            break;
                        default:
                            break;
                    }
                }
            }
            stream.Close();
            }
        }

        public IEnumerable<XElement> ScanGenerate()
        {
            foreach (string ffname in fogfile_names)
            {
            Stream stream = File.OpenRead(ffname);
            XmlReaderSettings settings = new XmlReaderSettings();
            settings.Async = false;

            using (XmlReader reader = XmlReader.Create(stream, settings))
            {
                XElement xrecord = null;
                XElement xelement = null;
                int level = 0; // Увеличивается при заходе в элемент и уменьшается при выходе.
                               // уровень сканирования: 0 - начало, 1 - rdf:RDF, 2 - записи, 3 - поля записей
                while (reader.Read())
                {
                    switch (reader.NodeType)
                    {
                        case XmlNodeType.Element:
                            level++;
                            if (level == 2) xrecord = new XElement(XName.Get(reader.LocalName, reader.NamespaceURI));
                            if (level == 3) xelement = new XElement(XName.Get(reader.LocalName, reader.NamespaceURI));
                            if (reader.HasAttributes)
                            {
                                for (int i = 0; i < reader.AttributeCount; i++)
                                {
                                    reader.MoveToAttribute(i);
                                    if (level == 2) xrecord.Add(new XAttribute(XName.Get(reader.LocalName, reader.NamespaceURI), reader.Value));
                                    if (level == 3) xelement.Add(new XAttribute(XName.Get(reader.LocalName, reader.NamespaceURI), reader.Value));
                                }
                                reader.MoveToElement(); // Moves the reader back to the element node.
                            }
                            if (reader.IsEmptyElement)
                            {
                                if (level == 3) { xrecord.Add(new XElement(xelement)); }
                                else if (level == 2)
                                {
                                    // Странная запись. Возможно substitute Рабочая зона
                                    throw new Exception("========= Strange ========" + xrecord);
                                }
                                level--;
                            }
                            break;
                        case XmlNodeType.Text:
                            if (level == 2)
                            {
                                xrecord.Add(reader.Value);
                            }
                            else if (level == 3)
                            {
                                xelement.Add(reader.Value);
                            }
                            break;
                        case XmlNodeType.EndElement:
                            string name = reader.Name;
                            if (level == 3) { xrecord.Add(new XElement(xelement)); }
                            else if (level == 2)
                            {
                                // Рабочая зона
                                var xx = transformXElement(xrecord);
                                if (xx != null) { yield return xx; }
                            }
                            level--;
                            break;
                        case XmlNodeType.Whitespace:
                            break;
                        default:
                            break;
                    }
                }
            }
            stream.Close();
            }
        }

    }
}

