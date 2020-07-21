namespace PolarDemo
{
    partial class Form1
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.splitContainer1 = new System.Windows.Forms.SplitContainer();
            this.treeView1 = new System.Windows.Forms.TreeView();
            this.tabControl1 = new System.Windows.Forms.TabControl();
            this.tpRun = new System.Windows.Forms.TabPage();
            this.tableLayoutPanel1 = new System.Windows.Forms.TableLayoutPanel();
            this.panel1 = new System.Windows.Forms.Panel();
            this.pFields = new System.Windows.Forms.Panel();
            this.panel2 = new System.Windows.Forms.Panel();
            this.lSampleName = new System.Windows.Forms.Label();
            this.bRun = new System.Windows.Forms.Button();
            this.tbConsole = new System.Windows.Forms.TextBox();
            this.tpCode = new System.Windows.Forms.TabPage();
            this.tbCode = new System.Windows.Forms.TextBox();
            this.tpDesc = new System.Windows.Forms.TabPage();
            this.descEdit = new System.Windows.Forms.RichTextBox();
            ((System.ComponentModel.ISupportInitialize)(this.splitContainer1)).BeginInit();
            this.splitContainer1.Panel1.SuspendLayout();
            this.splitContainer1.Panel2.SuspendLayout();
            this.splitContainer1.SuspendLayout();
            this.tabControl1.SuspendLayout();
            this.tpRun.SuspendLayout();
            this.tableLayoutPanel1.SuspendLayout();
            this.panel1.SuspendLayout();
            this.panel2.SuspendLayout();
            this.tpCode.SuspendLayout();
            this.tpDesc.SuspendLayout();
            this.SuspendLayout();
            // 
            // splitContainer1
            // 
            this.splitContainer1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.splitContainer1.Location = new System.Drawing.Point(0, 0);
            this.splitContainer1.Name = "splitContainer1";
            // 
            // splitContainer1.Panel1
            // 
            this.splitContainer1.Panel1.Controls.Add(this.treeView1);
            this.splitContainer1.Panel1.Padding = new System.Windows.Forms.Padding(0, 0, 0, 12);
            this.splitContainer1.Panel1MinSize = 250;
            // 
            // splitContainer1.Panel2
            // 
            this.splitContainer1.Panel2.Controls.Add(this.tabControl1);
            this.splitContainer1.Panel2MinSize = 650;
            this.splitContainer1.Size = new System.Drawing.Size(907, 472);
            this.splitContainer1.SplitterDistance = 250;
            this.splitContainer1.TabIndex = 0;
            // 
            // treeView1
            // 
            this.treeView1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.treeView1.Location = new System.Drawing.Point(0, 0);
            this.treeView1.Name = "treeView1";
            this.treeView1.Size = new System.Drawing.Size(250, 460);
            this.treeView1.TabIndex = 0;
            this.treeView1.NodeMouseClick += new System.Windows.Forms.TreeNodeMouseClickEventHandler(this.treeView1_NodeMouseClick);
            this.treeView1.NodeMouseDoubleClick += new System.Windows.Forms.TreeNodeMouseClickEventHandler(this.treeView1_NodeMouseDoubleClick);
            // 
            // tabControl1
            // 
            this.tabControl1.Controls.Add(this.tpRun);
            this.tabControl1.Controls.Add(this.tpCode);
            this.tabControl1.Controls.Add(this.tpDesc);
            this.tabControl1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.tabControl1.Location = new System.Drawing.Point(0, 0);
            this.tabControl1.Name = "tabControl1";
            this.tabControl1.SelectedIndex = 0;
            this.tabControl1.Size = new System.Drawing.Size(653, 472);
            this.tabControl1.TabIndex = 0;
            // 
            // tpRun
            // 
            this.tpRun.Controls.Add(this.tableLayoutPanel1);
            this.tpRun.Location = new System.Drawing.Point(4, 22);
            this.tpRun.Name = "tpRun";
            this.tpRun.Padding = new System.Windows.Forms.Padding(3);
            this.tpRun.Size = new System.Drawing.Size(645, 446);
            this.tpRun.TabIndex = 0;
            this.tpRun.Text = "Run";
            this.tpRun.UseVisualStyleBackColor = true;
            // 
            // tableLayoutPanel1
            // 
            this.tableLayoutPanel1.ColumnCount = 1;
            this.tableLayoutPanel1.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 50F));
            this.tableLayoutPanel1.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 50F));
            this.tableLayoutPanel1.Controls.Add(this.panel1, 0, 0);
            this.tableLayoutPanel1.Controls.Add(this.tbConsole, 0, 1);
            this.tableLayoutPanel1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.tableLayoutPanel1.Location = new System.Drawing.Point(3, 3);
            this.tableLayoutPanel1.Name = "tableLayoutPanel1";
            this.tableLayoutPanel1.RowCount = 2;
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 32.95454F));
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 67.04546F));
            this.tableLayoutPanel1.Size = new System.Drawing.Size(639, 440);
            this.tableLayoutPanel1.TabIndex = 0;
            // 
            // panel1
            // 
            this.panel1.BorderStyle = System.Windows.Forms.BorderStyle.FixedSingle;
            this.panel1.Controls.Add(this.pFields);
            this.panel1.Controls.Add(this.panel2);
            this.panel1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.panel1.Location = new System.Drawing.Point(3, 3);
            this.panel1.Name = "panel1";
            this.panel1.Size = new System.Drawing.Size(633, 138);
            this.panel1.TabIndex = 0;
            // 
            // pFields
            // 
            this.pFields.AutoScroll = true;
            this.pFields.BackColor = System.Drawing.Color.WhiteSmoke;
            this.pFields.BorderStyle = System.Windows.Forms.BorderStyle.FixedSingle;
            this.pFields.Dock = System.Windows.Forms.DockStyle.Fill;
            this.pFields.Location = new System.Drawing.Point(0, 29);
            this.pFields.Name = "pFields";
            this.pFields.Size = new System.Drawing.Size(631, 107);
            this.pFields.TabIndex = 2;
            // 
            // panel2
            // 
            this.panel2.Controls.Add(this.lSampleName);
            this.panel2.Controls.Add(this.bRun);
            this.panel2.Dock = System.Windows.Forms.DockStyle.Top;
            this.panel2.Location = new System.Drawing.Point(0, 0);
            this.panel2.Name = "panel2";
            this.panel2.Size = new System.Drawing.Size(631, 29);
            this.panel2.TabIndex = 1;
            // 
            // lSampleName
            // 
            this.lSampleName.AutoSize = true;
            this.lSampleName.Location = new System.Drawing.Point(3, 8);
            this.lSampleName.Name = "lSampleName";
            this.lSampleName.Size = new System.Drawing.Size(35, 13);
            this.lSampleName.TabIndex = 1;
            this.lSampleName.Text = "label1";
            // 
            // bRun
            // 
            this.bRun.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
            this.bRun.Location = new System.Drawing.Point(500, 3);
            this.bRun.Name = "bRun";
            this.bRun.Size = new System.Drawing.Size(111, 23);
            this.bRun.TabIndex = 0;
            this.bRun.Text = "Run Sample";
            this.bRun.UseVisualStyleBackColor = true;
            this.bRun.Click += new System.EventHandler(this.ProcessRun);
            // 
            // tbConsole
            // 
            this.tbConsole.BackColor = System.Drawing.SystemColors.WindowFrame;
            this.tbConsole.Dock = System.Windows.Forms.DockStyle.Fill;
            this.tbConsole.Font = new System.Drawing.Font("Calibri", 12F);
            this.tbConsole.ForeColor = System.Drawing.SystemColors.Window;
            this.tbConsole.Location = new System.Drawing.Point(3, 147);
            this.tbConsole.Multiline = true;
            this.tbConsole.Name = "tbConsole";
            this.tbConsole.ReadOnly = true;
            this.tbConsole.ScrollBars = System.Windows.Forms.ScrollBars.Vertical;
            this.tbConsole.Size = new System.Drawing.Size(633, 290);
            this.tbConsole.TabIndex = 1;
            // 
            // tpCode
            // 
            this.tpCode.Controls.Add(this.tbCode);
            this.tpCode.Location = new System.Drawing.Point(4, 22);
            this.tpCode.Name = "tpCode";
            this.tpCode.Padding = new System.Windows.Forms.Padding(3);
            this.tpCode.Size = new System.Drawing.Size(645, 446);
            this.tpCode.TabIndex = 1;
            this.tpCode.Text = "Show Code";
            this.tpCode.UseVisualStyleBackColor = true;
            // 
            // tbCode
            // 
            this.tbCode.BackColor = System.Drawing.SystemColors.ButtonHighlight;
            this.tbCode.Dock = System.Windows.Forms.DockStyle.Fill;
            this.tbCode.Location = new System.Drawing.Point(3, 3);
            this.tbCode.Multiline = true;
            this.tbCode.Name = "tbCode";
            this.tbCode.ReadOnly = true;
            this.tbCode.ScrollBars = System.Windows.Forms.ScrollBars.Vertical;
            this.tbCode.Size = new System.Drawing.Size(639, 440);
            this.tbCode.TabIndex = 0;
            // 
            // tpDesc
            // 
            this.tpDesc.Controls.Add(this.descEdit);
            this.tpDesc.Location = new System.Drawing.Point(4, 22);
            this.tpDesc.Name = "tpDesc";
            this.tpDesc.Size = new System.Drawing.Size(645, 446);
            this.tpDesc.TabIndex = 2;
            this.tpDesc.Text = "Description";
            this.tpDesc.UseVisualStyleBackColor = true;
            // 
            // descEdit
            // 
            this.descEdit.BackColor = System.Drawing.SystemColors.ButtonHighlight;
            this.descEdit.Dock = System.Windows.Forms.DockStyle.Fill;
            this.descEdit.Location = new System.Drawing.Point(0, 0);
            this.descEdit.Name = "descEdit";
            this.descEdit.ReadOnly = true;
            this.descEdit.Size = new System.Drawing.Size(645, 446);
            this.descEdit.TabIndex = 0;
            this.descEdit.Text = "";
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.AutoSize = true;
            this.ClientSize = new System.Drawing.Size(907, 472);
            this.Controls.Add(this.splitContainer1);
            this.Name = "Form1";
            this.StartPosition = System.Windows.Forms.FormStartPosition.Manual;
            this.Text = "Form1";
            this.WindowState = System.Windows.Forms.FormWindowState.Maximized;
            this.splitContainer1.Panel1.ResumeLayout(false);
            this.splitContainer1.Panel2.ResumeLayout(false);
            ((System.ComponentModel.ISupportInitialize)(this.splitContainer1)).EndInit();
            this.splitContainer1.ResumeLayout(false);
            this.tabControl1.ResumeLayout(false);
            this.tpRun.ResumeLayout(false);
            this.tableLayoutPanel1.ResumeLayout(false);
            this.tableLayoutPanel1.PerformLayout();
            this.panel1.ResumeLayout(false);
            this.panel2.ResumeLayout(false);
            this.panel2.PerformLayout();
            this.tpCode.ResumeLayout(false);
            this.tpCode.PerformLayout();
            this.tpDesc.ResumeLayout(false);
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.SplitContainer splitContainer1;
        private System.Windows.Forms.TreeView treeView1;
        private System.Windows.Forms.TabControl tabControl1;
        private System.Windows.Forms.TabPage tpRun;
        private System.Windows.Forms.TabPage tpCode;
        private System.Windows.Forms.TableLayoutPanel tableLayoutPanel1;
        private System.Windows.Forms.Panel panel1;
        private System.Windows.Forms.TextBox tbCode;
        private System.Windows.Forms.Button bRun;
        private System.Windows.Forms.TextBox tbConsole;
        private System.Windows.Forms.Panel panel2;
        private System.Windows.Forms.Label lSampleName;
        private System.Windows.Forms.Panel pFields;
        private System.Windows.Forms.TabPage tpDesc;
        private System.Windows.Forms.RichTextBox descEdit;
    }
}

