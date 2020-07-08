using Polar.Common;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Resources;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

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
                AddSubNodes(node, li, relatedNS, nsSamples, ss.Item1, ss.Item2);
                node.Nodes.AddRange(ss.Item1.Where(t => t.Namespace == li).Select(t => PrepareSample(t)).
                    OrderBy(s => s.DiplayName).Select(s => new TreeNode(s.DiplayName) 
                    { Tag = Tuple.Create(s, GetSourceCode(s.Name, ss.Item2)) }).ToArray());
            }                            
        }

        private void AddSubNodes(TreeNode node, string parrentNs, List<string> usedNs, 
            List<string> sampledNS, List<Type> samples, Dictionary<string, string> resources1)
        {
            if (sampledNS == null || sampledNS.Count == 0) { return; }
            var childrenNs = sampledNS.Where(s => s.StartsWith(parrentNs) && s.Length > parrentNs.Length).ToList();
            foreach (var li in childrenNs)
            {
                if (usedNs.Contains(li)) { continue; }
                usedNs.Add(li);
                var curnode = node.Nodes.Add(li.Replace(parrentNs, "").Substring(1));
                AddSubNodes(curnode, li, usedNs, childrenNs, samples, resources1);
                curnode.Nodes.AddRange(samples.Where(t => t.Namespace == li).Select(t => PrepareSample(t)).
                    OrderBy(s => s.DiplayName).Select(s => new TreeNode(s.DiplayName) 
                    { Tag = Tuple.Create(s, GetSourceCode(s.Name, resources1)) }).ToArray());
            }
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

        private void ProcessRun(object sender, EventArgs e)
        {
            tbConsole.Text = string.Empty;
            tbConsole.ResumeLayout();
            if (currentSample == null)
            {
                tbConsole.Text = "Select Sample first";
                return;
            }
            var stdOut = Console.Out;
            var consoleOut = new StringWriter();
            Console.SetOut(consoleOut);

            //Run Sample
            currentSample.Item1.Fields = GetFields();
            currentSample.Item1.Run();

            var builder = consoleOut.GetStringBuilder();
            tbConsole.Text = builder.ToString();
            Console.SetOut(stdOut);
        }

        private ICollection<IField> GetFields()
        {
            return new List<IField>();
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

        private Tuple<List<Type>, Dictionary<string, string>> GetAssembiles()
        {
            var sampleTypes = new List<Type>();
            var sources = new Dictionary<string, string>();
            var path = Path.GetDirectoryName(Application.ExecutablePath);
            foreach(var f in Directory.GetFiles(path, "*.dll"))
            {
                PrepareFile(f, sampleTypes, sources);
            }
            foreach (var f in Directory.GetFiles(path, "*.exe"))
            {
                PrepareFile(f, sampleTypes, sources);
            }
            return Tuple.Create(sampleTypes, sources);
        }

        private void PrepareFile(string f, List<Type> sampleTypes, Dictionary<string, string> sources)
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
        private Tuple<ISample, string> currentSample;
        private void DrawNodeCore(TreeNode node) {
            GetNodeSample(node);
          
            if (currentSample == null)
            {
                tbCode.Text = "";
                lSampleName.Text = "";
                bRun.Enabled = false;
            }
            else
            {
                tbCode.Text = currentSample.Item2;
                lSampleName.Text = currentSample.Item1.DiplayName;
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
        }

        private void GetNodeSample(TreeNode node)
        {
            if (node == null || node.Nodes.Count > 0)
            {
                currentSample = null;
                return;
            }
            var tag = node.Tag as Tuple<ISample, string>;
            currentSample = tag == null || string.IsNullOrEmpty(tag.Item2) ? null : tag;
        }
    }
}
