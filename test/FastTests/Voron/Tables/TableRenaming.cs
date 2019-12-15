using System;
using System.Linq;
using Voron.Global;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Voron.Tables
{
    public class TableRenaming : TableStorageTest
    {
        public TableRenaming(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanRenameTable()
        {
            const string tableName = "docs";
            using (var tx = Env.WriteTransaction())
            {
                DocsSchema.Create(tx, tableName, 16);
                tx.Commit();
            }

            var largeString = new string('a', 1024);
            using (var tx = Env.WriteTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, tableName);

                for (int i = 0; i < 250; i++)
                {
                    SetHelper(docs, "users/" + i, "Users", 1L + i, largeString);
                }

                tx.Commit();
            }

            var tempTableName = $"temp_{tableName}";
            using (var tx = Env.WriteTransaction())
            {
                tx.RenameTable(tableName, tempTableName);
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var oldTable = tx.OpenTable(DocsSchema, tableName);
                Assert.Null(oldTable);

                var newTable = tx.OpenTable(DocsSchema, tempTableName);
                Assert.NotNull(newTable);

                foreach (var index in DocsSchema.Indexes)
                {
                    var tree = newTable.GetTree(index.Value);
                    Assert.NotEqual(1, tree.State.Depth);
                    var pages = tree.AllPages();
                    var minPage = pages.Min();
                    var maxPage = pages.Max();
                    Assert.True((maxPage - minPage) < 128);
                }
            }
        }

        [Fact]
        public void ShouldNotAllowToRenameTableIfTableAlreadyExists()
        {
            using (var tx = Env.WriteTransaction())
            {
                const string tableName1 = "table_1";
                const string tableName2 = "table_2";
                tx.CreateTree(tableName1);
                tx.CreateTree(tableName2);

                var ae = Assert.Throws<ArgumentException>(() => tx.RenameTable(tableName1, tableName2));

                Assert.Equal($"Cannot rename a table with the name of an existing table: {tableName2}", ae.Message);
            }
        }

        [Fact]
        public void ShouldThrowIfTableDoesNotExist()
        {
            using (var tx = Env.WriteTransaction())
            {
                const string tableName = "table_1";
                var ae = Assert.Throws<ArgumentException>(() => tx.RenameTable(tableName, "table_2"));

                Assert.Equal($"Table {tableName} does not exist", ae.Message);
            }
        }

        [Fact]
        public void MustNotRenameToRootAndFreeSpaceRootTrees()
        {
            using (var tx = Env.WriteTransaction())
            {
                var ex = Assert.Throws<InvalidOperationException>(() => tx.RenameTable("table_1", Constants.RootTreeName));
                Assert.Equal("Cannot create a table with reserved name: " + Constants.RootTreeName, ex.Message);
            }
        }

        [Fact]
        public void ShouldPreventFromRenamingTableInReadTransaction()
        {
            using (var tx = Env.ReadTransaction())
            {
                var ae = Assert.Throws<ArgumentException>(() => tx.RenameTable("table_1", "table_2"));

                Assert.Equal("Cannot rename a new table with a read only transaction", ae.Message);
            }
        }
    }
}
