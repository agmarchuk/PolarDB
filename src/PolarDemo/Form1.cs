using Polar.Common;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Resources;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.Windows.Threading;

namespace PolarDemo
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
            
            lSampleName.Text = "";
            bRun.Enabled = false;
            tbConsole.Text = "";
            tbCode.Text = "";

            var resourses = new List<Type>();
            var ss = GetAssembiles();

            var relatedNS = new List<string>();
            var nsSamples = ss.Item1.OrderBy(t => t.Namespace).Select(t => t.Namespace).Distinct().ToList();
            foreach (var li in nsSamples) {
                if (relatedNS.Contains(li)) { continue; }
                relatedNS.Add(li);
                var node = treeView1.Nodes.Add(li);
                AddSubNodes(node, li, relatedNS, nsSamples, ss.Item1, ss.Item2, ss.Item3);
                node.Nodes.AddRange(ss.Item1.Where(t => t.Namespace == li).Select(t => PrepareSample(t)).
                    OrderBy(s => s.DiplayName).Select(s => new TreeNode(s.DiplayName) 
                    { Tag = Tuple.Create(s, GetSourceCode(s.Name, ss.Item2), GetDescription(s.Name, ss.Item3)) }).ToArray());
            }
            testRun = false;
        }

        private void AddSubNodes(TreeNode node, string parrentNs, List<string> usedNs, 
            List<string> sampledNS, List<Type> samples, Dictionary<string, string> resources1, Dictionary<string, string> resource2)
        {
            if (sampledNS == null || sampledNS.Count == 0) { return; }
            var childrenNs = sampledNS.Where(s => s.StartsWith(parrentNs) && s.Length > parrentNs.Length).ToList();
            foreach (var li in childrenNs)
            {
                if (usedNs.Contains(li)) { continue; }
                usedNs.Add(li);
                var curnode = node.Nodes.Add(li.Replace(parrentNs, "").Substring(1));
                AddSubNodes(curnode, li, usedNs, childrenNs, samples, resources1, resource2);
                curnode.Nodes.AddRange(samples.Where(t => t.Namespace == li).Select(t => PrepareSample(t)).
                    OrderBy(s => s.DiplayName).Select(s => new TreeNode(s.DiplayName) 
                    { Tag = Tuple.Create(s, GetSourceCode(s.Name, resources1), GetDescription(s.Name, resource2)) }).ToArray());
            }
        }

        private string GetDescription(string name, Dictionary<string, string> resources)
        {
            if (string.IsNullOrEmpty(name) || resources == null || !resources.ContainsKey(name))
            {
                return "Description is not available";
            }
            return resources[name];
        }

        private string GetSourceCode(string name, Dictionary<string, string> resources)
        {
            if(string.IsNullOrEmpty(name) || resources == null || !resources.ContainsKey(name))
            {
                return "Code is not available";
            }
            var s =  resources[name];
            if (string.IsNullOrEmpty(s)) { return "Code is not available"; }
            var sind = s.IndexOf("//START_SOURCE_CODE");
            
            if (sind < 0) { return s; }
            var s1 = s.Substring(sind + "//START_SOURCE_CODE".Length);
            var eind = s1.IndexOf("//END_SOURCE_CODE");
            if (eind < 0) { return s; }
            return s1.Substring(0, eind).Trim();
        }
        private bool testRun;
        private void ProcessRun(object sender, EventArgs e)
        {
            if (testRun) { return; }

                tbConsole.Text = string.Empty;
                tbConsole.ResumeLayout();
                if (currentSample == null)
                {
                    tbConsole.Text = "Select Sample first";
                    testRun = false;
                    return;
                }
            TextWriter stdOut = null;
            Stopwatch sw = new Stopwatch();
            try
            {
                stdOut = Console.Out;
                var consoleOut = new StringWriter();
                Console.SetOut(consoleOut);

                //Run Sample
                SaveFields(currentSample.Item1);

                testRun = true;
                var thread = new Thread(TestAppRun);
                sw.Start();
                thread.Start();

                while (testRun)
                {
                    tbConsole.Text += "ping\r\n";
                    Thread.Sleep(200);
                    var builder = consoleOut.GetStringBuilder();
                    tbConsole.Text = builder.ToString();
                    tbConsole.Refresh();
                }
            }
            finally
            {
                sw.Stop();
                if (stdOut != null) { Console.SetOut(stdOut); }
                MessageBox.Show($@"Time of test = {sw.ElapsedMilliseconds} ms");
            }
        }
        
        private void SaveFields(ISample sample)
        {
            if (sample == null || sample.Fields == null || sample.Fields.Count == 0) { return; }
            foreach(var control in pFields.Controls)
            {
                if (control is NumericUpDown nud)
                {
                    SetFieldValue(nud, sample);
                }
                else if (control is DateTimePicker dtp)
                {
                    SetFieldValue(sample, typeof(DateTime), dtp.Value, dtp.Tag as string);
                }
                else if (control is TextBox tb)
                {
                    SetFieldValue(sample, typeof(string), tb.Text, tb.Tag as string);
                }
                else if (control is CheckBox cb)
                {
                    SetFieldValue(sample, typeof(bool), cb.Checked, cb.Tag as string);
                }
            }
        }

        private void SetFieldValue(ISample sample, Type condType, object value, string name)
        {
            if (string.IsNullOrEmpty(name)) { return; }
            var type = sample.GetType();
            var property = type.GetProperty(name);
            var field = type.GetField(name);
            if (property == null && field == null) { return; }
            Type compType = property == null ? field.FieldType : property.PropertyType;
            Action<object, object> setAction = null;
            if (property == null) { setAction = (s, v) => field.SetValue(s, v); }
            else { setAction = (s, v) => property.SetValue(s, v); }

            if (compType != condType) { return; }
            setAction(sample, value);
        }

        private void SetFieldValue(NumericUpDown nud, ISample sample)
        {
            var name = nud.Tag as string;
            if (string.IsNullOrEmpty(name)) { return; }
            var type = sample.GetType();
            var property = type.GetProperty(name);
            var field = type.GetField(name);
            if (property == null && field == null) { return; }
            Type compType = property == null ? field.FieldType : property.PropertyType;
            Action<object, object> setAction = null;
            if (property == null) { setAction = (s, v) => field.SetValue(s, v); }
            else { setAction = (s, v) => property.SetValue(s, v); }

            bool found = false;
            object value = null;
            try
            {
                if (compType == typeof(int))
                {
                    found = true;
                    value = Convert.ToInt32(nud.Value);
                }
                if (compType == typeof(byte))
                {
                    found = true;
                    value = Convert.ToByte(nud.Value);
                }
                if (compType == typeof(long))
                {
                    found = true;
                    value = Convert.ToInt64(nud.Value);
                }
                if (compType == typeof(decimal))
                {
                    found = true;
                    value = Convert.ToDecimal(nud.Value);
                }
                if (compType == typeof(double))
                {
                    found = true;
                    value = Convert.ToDouble(nud.Value);
                }
                if (found)
                {
                    setAction(sample, value);
                }
            }
            catch { }
        }

        private void TestAppRun()
        {
            try
            {
                currentSample.Item1.Run();
                currentSample.Item1.Clear();
            }
            finally
            {
                testRun = false;
            }
        }

        private ISample PrepareSample(Type t)
        {
            try
            {
                ISample sample = Activator.CreateInstance(t) as ISample;
                if (sample != null) { sample.Name = t.Name; }
                return sample;
            }
            catch { return null; }
        }

        private Tuple<List<Type>, Dictionary<string, string>, Dictionary<string, string>> GetAssembiles()
        {
            var sampleTypes = new List<Type>();
            var sources = new Dictionary<string, string>();
            var descriptions = new Dictionary<string, string>();
            var path = Path.GetDirectoryName(Application.ExecutablePath);
            foreach(var f in Directory.GetFiles(path, "*.dll"))
            {
                PrepareFile(f, sampleTypes, sources, descriptions);
            }
            foreach (var f in Directory.GetFiles(path, "*.exe"))
            {
                PrepareFile(f, sampleTypes, sources, descriptions);
            }
            return Tuple.Create(sampleTypes, sources, descriptions);
        }

        private void PrepareFile(string f, List<Type> sampleTypes, 
            Dictionary<string, string> sources, Dictionary<string, string> descriptions)
        {
            var ass = Assembly.LoadFile(Path.GetFullPath(f));
            var names = ass.GetManifestResourceNames();
            var types = ass.GetTypes().Where(t => t.GetInterface("Polar.Common.ISample") != null).ToList();
            if (types == null || types.Count == 0) { return; }
            ResourceManager rs = null;
            if (names != null && names.Length > 0)
            {
                rs = new System.Resources.ResourceManager(ass.GetName().Name + ".Properties.Resources", ass);
            }
            foreach (var t in types)
            {
                sampleTypes.Add(t);
                if (rs != null)
                {
                    var sam = rs.GetString(t.Name);
                    if (!string.IsNullOrEmpty(sam) && !sources.ContainsKey(t.Name))
                    {
                        sources.Add(t.Name, sam);
                    }
                    sam = rs.GetString(t.Name + "_desc");
                    if (!string.IsNullOrEmpty(sam) && !descriptions.ContainsKey(t.Name))
                    {
                        descriptions.Add(t.Name, sam);
                    }
                }
            }
        }

        private void treeView1_NodeMouseClick(object sender, TreeNodeMouseClickEventArgs e)
        {
            DrawNodeCore(e.Node);
        }

        private void treeView1_NodeMouseDoubleClick(object sender, TreeNodeMouseClickEventArgs e)
        {
            DrawNodeCore(e.Node);
        }
        private Tuple<ISample, string, string> currentSample;
        private void DrawNodeCore(TreeNode node) {
            GetNodeSample(node);
          
            if (currentSample == null)
            {
                tbCode.Text = "";
                lSampleName.Text = "";
                pFields.Controls.Clear();
                bRun.Enabled = false;
                descEdit.Text = "";
            }
            else
            {
                tbCode.Text = currentSample.Item2;
                lSampleName.Text = currentSample.Item1.DiplayName;
                descEdit.Text = currentSample.Item3;
                bRun.Enabled = true;
            }
            PrepareFields();
        }

        private void PrepareFields()
        {
            pFields.Controls.Clear();
            if(currentSample == null || currentSample.Item1.Fields == null || currentSample.Item1.Fields.Count == 0)
            {
                return;
            }
            int i = 0;
            foreach(var f in currentSample.Item1.Fields)
            {
                Control control = null;
                
                if(f is NumericField nfield)
                {
                    control = new NumericUpDown();
                    ((NumericUpDown)control).Maximum = 99_999_9999_9999_999;
                    if (f.DefaultValue == null) { ((NumericUpDown)control).Value = 0; }
                    else
                    {
                        if (f.DefaultValue is decimal) { ((NumericUpDown)control).Value = (decimal)f.DefaultValue; }
                        else if (f.DefaultValue is long) { ((NumericUpDown)control).Value = (long)f.DefaultValue; }
                        else if (f.DefaultValue is int) { ((NumericUpDown)control).Value = (int)f.DefaultValue; }
                        else if (f.DefaultValue is byte) { ((NumericUpDown)control).Value = (byte)f.DefaultValue; }
                    }
                    control.Location = new Point((i / 5 * 280) + 150, i % 5 * 23 + 3);
                }
                else if (f is StringField sfield)
                {
                    control = new TextBox();
                    ((TextBox)control).Text = f.DefaultValue != null && f.DefaultValue is string
                        ? (string)f.DefaultValue : "";
                    control.Location = new Point((i / 5 * 280) + 150, i % 5 * 23 + 3);
                }
                else if (f is DateField dfield)
                {
                    control = new DateTimePicker();
                    DateTime d = DateTime.Now;
                    if (f.DefaultValue != null && f.DefaultValue is string)
                    {
                        DateTime.TryParse((string)f.DefaultValue, out d);
                    }
                    ((DateTimePicker)control).Value = d;
                    ((DateTimePicker)control).Format = DateTimePickerFormat.Short;
                    control.Location = new Point((i / 5 * 280) + 150, i % 5 * 23 + 3);
                }
                else if(f is BooleanField bfield)
                {
                    control = new CheckBox();
                    control.Location = new Point((i / 5 * 280) + 150, i % 5 * 23);
                    ((CheckBox)control).Checked = f.DefaultValue != null && f.DefaultValue is bool
                        ? (bool)f.DefaultValue : false;
                }
                if (control != null)
                {
                    
                    control.Name = "control" + i;
                    control.Size = new System.Drawing.Size(100, 23);
                    control.Tag = f.Name;
                    pFields.Controls.Add(control);

                    var label1 = new Label();
                    label1.AutoSize = true;
                    label1.Location = new System.Drawing.Point((i / 5 * 280) + 3, i % 5 * 23 + 5);
                    label1.Name = "label" + i;
                    label1.Size = new System.Drawing.Size(60, 13);
                    label1.Text = f.LabelName;
                    pFields.Controls.Add(label1);
                    i++;
                }
            }
            pFields.ResumeLayout();
        }

        private void GetNodeSample(TreeNode node)
        {
            if (node == null || node.Nodes.Count > 0)
            {
                currentSample = null;
                return;
            }
            var tag = node.Tag as Tuple<ISample, string, string>;
            currentSample = tag == null || string.IsNullOrEmpty(tag.Item2) ? null : tag;
        }
    }
}
