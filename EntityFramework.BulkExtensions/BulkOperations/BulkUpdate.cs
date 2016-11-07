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
    /// <summary>
    /// 
    /// </summary>
    public class BulkUpdate : IBulkOperation
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="context"></param>
        /// <param name="collection"></param>
        /// <param name="columnDirection"></param>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        public int CommitTransaction<T>(DbContext context, IEnumerable<T> collection, ColumnDirection columnDirection = ColumnDirection.Input) where T : class
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
                var command = context.BuildCreateTempTable<T>(tmpTableName, columnDirection);
                database.ExecuteSqlCommand(command);

                //Bulk insert into temp table
                database.BulkInsertToTable(dataTable, tmpTableName, SqlBulkCopyOptions.Default);

                //command = SqlHelper.GetOutputCreateTableCmd(columnDirection, Constants.TempOutputTableName,
                //OperationType.InsertOrUpdate, context.GetTablePKs<T>().First());

                //if (!string.IsNullOrWhiteSpace(command))
                //{
                //    database.ExecuteSqlCommand(command);
                //}

                // Updating destination table, and dropping temp table
                command = "MERGE INTO " + context.GetTableName<T>() + " WITH (HOLDLOCK) AS Target " +
                          "USING " + tmpTableName + " AS Source " +
                          context.PrimaryKeysComparator<T>() +
                          "WHEN MATCHED " +
                          "THEN UPDATE " +
                          context.BuildUpdateSet<T>() + "; " +
                          //SqlHelper.GetOutputIdentityCmd(tmpTableName, context.GetTablePKs<T>().First(), ColumnDirection.Input, OperationType.Update) + "; " +
                          SqlHelper.GetDropTableCommand(tmpTableName);

                affectedRows = database.ExecuteSqlCommand(command);

                //if (columnDirection == ColumnDirection.InputOutput)
                //{
                //    database.LoadFromTmpOutputTable(context.GetTablePKs<T>().First(), columnDirection,
                //        OperationType.Update, entityList);
                //}

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
