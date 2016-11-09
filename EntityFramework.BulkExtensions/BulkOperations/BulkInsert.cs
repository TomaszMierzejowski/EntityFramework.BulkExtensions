using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using EntityFramework.BulkExtensions.Extensions;
using EntityFramework.BulkExtensions.Helpers;
using EntityFramework.BulkExtensions.Operations;
using SqlBulkTools;

namespace EntityFramework.BulkExtensions.BulkOperations
{
    public class BulkInsert : IBulkOperation
    {
        public int CommitTransaction<T>(DbContext context, IEnumerable<T> collection, Identity identity) where T : class
        {
            var tmpTableName = context.RandomTableName<T>();
            var entityList = collection.ToList();
            var database = context.Database;
            var affectedRows = 0;
            if (!entityList.Any())
            {
                return affectedRows;
            }
            DbContextTransaction transaction = null;
            if (database.CurrentTransaction == null)
            {
                transaction = database.BeginTransaction();
            }
            try
            {
                var dataTable = context.ToDataTable(entityList);
                //Creating temp table on database

                if (identity == Identity.InputOutput)
                {
                    var command = context.BuildCreateTempTable<T>(tmpTableName, identity);
                    database.ExecuteSqlCommand(command);

                    database.BulkInsertToTable(dataTable, tmpTableName, SqlBulkCopyOptions.Default);

                    var tmpOutputTableName = context.RandomTableName<T>();
                    var commandText = context.GetInsertIntoStagingTableCmd<T>(tmpOutputTableName, tmpTableName, context.GetTablePKs<T>().First());
                    database.ExecuteSqlCommand(commandText);

                    database.LoadFromTmpOutputTable(tmpOutputTableName, context.GetTablePKs<T>().First(), entityList);
                }
                else
                {
                    database.BulkInsertToTable(dataTable, context.GetTableName<T>(), SqlBulkCopyOptions.Default);
                }

                affectedRows = dataTable.Rows.Count;
                return affectedRows;
            }

            catch (Exception)
            {
                transaction?.Rollback();
                throw;
            }
            finally
            {
                transaction?.Commit();
            }
        }

        public Task<int> CommitTransactionAsync<T>(DbContext context, IEnumerable<T> collection)
        {
            throw new NotImplementedException();
        }
    }
}