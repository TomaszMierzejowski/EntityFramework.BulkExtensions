using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using EntityFramework.BulkExtensions.Extensions;
using EntityFramework.BulkExtensions.Helpers;
using SqlBulkTools;

namespace EntityFramework.BulkExtensions.BulkOperations
{
    public class BulkInsert : IBulkOperation
    {
        public int CommitTransaction<T>(DbContext context, IEnumerable<T> collection, ColumnDirection columnDirection) where T : class
        {
            var tmpTableName = SqlHelper.RandomTableName();
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

                if (columnDirection == ColumnDirection.InputOutput)
                {
                    var command = context.BuildCreateTempTable<T>(tmpTableName, columnDirection);
                    database.ExecuteSqlCommand(command);

                    database.BulkInsertToTable(dataTable, tmpTableName, SqlBulkCopyOptions.Default);

                    var tmpOutputTableName = SqlHelper.RandomTableName();
                    var commandText = context.GetInsertIntoStagingTableCmd<T>(tmpOutputTableName, tmpTableName, context.GetTablePKs<T>().First(), columnDirection);
                    database.ExecuteSqlCommand(commandText);

                    database.LoadFromTmpOutputTable(tmpOutputTableName, tmpTableName, context.GetTablePKs<T>().First(), null, OperationType.Insert, collection);
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