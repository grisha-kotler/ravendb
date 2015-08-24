﻿// -----------------------------------------------------------------------
//  <copyright file="SimpleFixedSizeTrees.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Xunit;

namespace Voron.Tests.FixedSize
{
    public class SimpleFixedSizeTrees : StorageTest
    {
        [Fact]
        public void TimeSeries()
        {
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var fst = tx.State.Root.FixedTreeFor("watches/12831-12345", valSize: 8);

                fst.Add(DateTime.Today.AddHours(8).Ticks, new Slice(BitConverter.GetBytes(80D)));
                fst.Add(DateTime.Today.AddHours(9).Ticks, new Slice(BitConverter.GetBytes(65D)));
                fst.Add(DateTime.Today.AddHours(10).Ticks, new Slice(BitConverter.GetBytes(44D)));

                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            {
                var fst = tx.State.Root.FixedTreeFor("watches/12831-12345", valSize: 8);

                var it = fst.Iterate();
                Assert.True(it.Seek(DateTime.Today.AddHours(7).Ticks));
                var buffer = new byte[8];
                it.Value.CopyTo(buffer);
                Assert.Equal(80D, BitConverter.ToDouble(buffer, 0));
                Assert.True(it.MoveNext());
                it.Value.CopyTo(buffer);
                Assert.Equal(65D, BitConverter.ToDouble(buffer, 0));
                Assert.True(it.MoveNext());
                it.Value.CopyTo(buffer);
                Assert.Equal(44d, BitConverter.ToDouble(buffer, 0));
                Assert.False(it.MoveNext());

                tx.Commit();
            }
        }

        [Fact]
        public void CanAdd()
        {
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var fst = tx.State.Root.FixedTreeFor("test");

                fst.Add(1);
                fst.Add(2);

                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            {
                var fst = tx.State.Root.FixedTreeFor("test");

                Assert.True(fst.Contains(1));
                Assert.True(fst.Contains(2));
                Assert.False(fst.Contains(3));
                tx.Commit();
            }
        }

		[Fact]
		public void SeekShouldGiveTheNextKey()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var fst = tx.State.Root.FixedTreeFor("test");

				fst.Add(635634432000000000);
				fst.Add(635634468000000000);
				fst.Add(635634504000000000);

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				var it = tx.State.Root.FixedTreeFor("test").Iterate();

				Assert.True(it.Seek(635634432000000000));
				Assert.Equal(635634432000000000, it.CurrentKey);
				Assert.True(it.Seek(635634468000000000));
				Assert.Equal(635634468000000000, it.CurrentKey);
				Assert.True(it.Seek(635634504000000000));
				Assert.Equal(635634504000000000, it.CurrentKey);
				Assert.False(it.Seek(635634504000000001));
				tx.Commit();
			}
		}

        [Fact]
        public void CanAdd_Mixed()
        {
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var fst = tx.State.Root.FixedTreeFor("test");

                fst.Add(2);
                fst.Add(6);
                fst.Add(1);
                fst.Add(3);
                fst.Add(-3);

                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            {
                var fst = tx.State.Root.FixedTreeFor("test");

                Assert.True(fst.Contains(1));
                Assert.True(fst.Contains(2));
                Assert.False(fst.Contains(5));
                Assert.True(fst.Contains(6));
                Assert.False(fst.Contains(4));
                Assert.True(fst.Contains(-3));
                Assert.True(fst.Contains(3));
                tx.Commit();
            }
        }

        [Fact]
        public void CanIterate()
        {
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var fst = tx.State.Root.FixedTreeFor("test");

                fst.Add(3);
                fst.Add(1);
                fst.Add(2);

                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            {
                var fst = tx.State.Root.FixedTreeFor("test");

                var it = fst.Iterate();
                Assert.True(it.Seek(long.MinValue));
                Assert.Equal(1L, it.CurrentKey);
                Assert.True(it.MoveNext());
                Assert.Equal(2L, it.CurrentKey);
                Assert.True(it.MoveNext());
                Assert.Equal(3L, it.CurrentKey);
                Assert.False(it.MoveNext());


                tx.Commit();
            }
        }


        [Fact]
        public void CanRemove()
        {
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var fst = tx.State.Root.FixedTreeFor("test");

                fst.Add(1);
                fst.Add(2);
                fst.Add(3);
                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var fst = tx.State.Root.FixedTreeFor("test");

                fst.Delete(2);

                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            {
                var fst = tx.State.Root.FixedTreeFor("test");

                Assert.True(fst.Contains(1));
                Assert.False(fst.Contains(2));
                Assert.True(fst.Contains(3));
                tx.Commit();
            }
        }

		[Fact]
		public void CanDeleteRange()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var fst = tx.State.Root.FixedTreeFor("test", 8);

				for (int i = 1; i <= 10; i++)
				{
					fst.Add(i, new Slice(BitConverter.GetBytes(i + 10L)));
				}
				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var fst = tx.State.Root.FixedTreeFor("test", 8);

				var itemsRemoved = fst.DeleteRange(2, 5);
				Assert.Equal(4, itemsRemoved.NumberOfEntriesDeleted);
				Assert.Equal(false, itemsRemoved.TreeRemoved);

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				var fst = tx.State.Root.FixedTreeFor("test", 8);

				for (int i = 1; i <= 10; i++)
				{
					if (i >= 2 && i <= 5)
					{
						Assert.False(fst.Contains(i), i.ToString());
						Assert.Null(fst.Read(i));
					}
					else
					{
						Assert.True(fst.Contains(i), i.ToString());
						Assert.Equal(i + 10L, fst.Read(i).CreateReader().ReadLittleEndianInt64());
					}
				}
				tx.Commit();
			}
		}

		[Fact]
		public void CanDeleteRangeWithGaps()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var fst = tx.State.Root.FixedTreeFor("test", 8);

				for (int i = 1; i <= 10; i++)
				{
					fst.Add(i, new Slice(BitConverter.GetBytes(i + 10L)));
				}
				for (int i = 30; i <= 40; i++)
				{
					fst.Add(i, new Slice(BitConverter.GetBytes(i + 10L)));
				}
				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var fst = tx.State.Root.FixedTreeFor("test", 8);

				var itemsRemoved = fst.DeleteRange(2, 35);
				Assert.Equal(15, itemsRemoved.NumberOfEntriesDeleted);
				Assert.Equal(false, itemsRemoved.TreeRemoved);

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				var fst = tx.State.Root.FixedTreeFor("test", 8);

				for (int i = 1; i <= 10; i++)
				{
					if (i >= 2)
					{
						Assert.False(fst.Contains(i), i.ToString());
						Assert.Null(fst.Read(i));
					}
					else
					{
						Assert.True(fst.Contains(i), i.ToString());
						Assert.Equal(i + 10L, fst.Read(i).CreateReader().ReadLittleEndianInt64());
					}
				}
				for (int i = 30; i <= 40; i++)
				{
					if (i <= 35)
					{
						Assert.False(fst.Contains(i), i.ToString());
						Assert.Null(fst.Read(i));
					}
					else
					{
						Assert.True(fst.Contains(i), i.ToString());
						Assert.Equal(i + 10L, fst.Read(i).CreateReader().ReadLittleEndianInt64());
					}
				}
				tx.Commit();
			}
		}

		[Fact]
		public void CanDeleteAllRange()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var fst = tx.State.Root.FixedTreeFor("test", 8);

				for (int i = 1; i <= 10; i++)
				{
					fst.Add(i, new Slice(BitConverter.GetBytes(i + 10L)));
				}
				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var fst = tx.State.Root.FixedTreeFor("test", 8);

				var itemsRemoved = fst.DeleteRange(0, DateTime.MaxValue.Ticks);
				Assert.Equal(10, itemsRemoved.NumberOfEntriesDeleted);
				Assert.Equal(true, itemsRemoved.TreeRemoved);

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				var fst = tx.State.Root.FixedTreeFor("test", 8);

				for (int i = 1; i <= 10; i++)
				{
					Assert.False(fst.Contains(i), i.ToString());
					Assert.Null(fst.Read(i));
				}
				tx.Commit();
			}
		}

        [Fact]
        public void CanAdd_WithValue()
        {
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var fst = tx.State.Root.FixedTreeFor("test", 8);

                fst.Add(1, new Slice(BitConverter.GetBytes(1L)));
                fst.Add(2, new Slice(BitConverter.GetBytes(2L)));

                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            {
                var fst = tx.State.Root.FixedTreeFor("test", 8);

                Assert.Equal(1L, fst.Read(1).CreateReader().ReadLittleEndianInt64());
                Assert.Equal(2L, fst.Read(2).CreateReader().ReadLittleEndianInt64());
                Assert.Null(fst.Read(3));
                tx.Commit();
            }
        }

        [Fact]
        public void CanRemove_WithValue()
        {
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var fst = tx.State.Root.FixedTreeFor("test", 8);

                fst.Add(1, new Slice(BitConverter.GetBytes(1L)));
                fst.Add(2, new Slice(BitConverter.GetBytes(2L)));
                fst.Add(3, new Slice(BitConverter.GetBytes(3L)));

                tx.Commit();
            }


            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var fst = tx.State.Root.FixedTreeFor("test", 8);

                fst.Delete(2);

                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            {
                var fst = tx.State.Root.FixedTreeFor("test", 8);

                Assert.Equal(1L, fst.Read(1).CreateReader().ReadLittleEndianInt64());
                Assert.Null(fst.Read(2));
                Assert.Equal(3L, fst.Read(3).CreateReader().ReadLittleEndianInt64());
                tx.Commit();
            }
        }
    }
}