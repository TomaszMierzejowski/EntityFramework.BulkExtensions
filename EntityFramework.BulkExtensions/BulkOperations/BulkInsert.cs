using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using EntityFramework.BulkExtensions.Extensions;
using EntityFramework.BulkExtensions.Helpers;
using EntityFramework.BulkExtensions.Operations;

namespace EntityFramework.BulkExtensions.BulkOperations
{
    public class BulkInsert : IBulkOperation
    {
        int IBulkOperation.CommitTransaction<T>(DbContext context, IEnumerable<T> collection, Identity identity)
        {
            var tmpTableName = context.RandomTableName<T>();
            var entityList = collection.ToList();
            var database = context.Database;
            var affectedRows = 0;
            if (!entityList.Any())
            {
                return affectedRows;
            }

            //Creates inner transaction if the context doens't have one.
            var transaction = context.InternalTransaction();
            try
            {
                //Cconvert entity collection into a DataTable
                var dataTable = context.ToDataTable(entityList);

                //Return generated IDs for bulk inserted elements.
                if (identity == Identity.InputOutput)
                {
                    //Create temporary table.
                    var command = context.BuildCreateTempTable<T>(tmpTableName);
                    database.ExecuteSqlCommand(command);

                    //Bulk inset data to temporary temporary table.
                    database.BulkInsertToTable(dataTable, tmpTableName, SqlBulkCopyOptions.Default);

                    var tmpOutputTableName = context.RandomTableName<T>();
                    //Copy data from temporary table to destination table with ID output to another temporary table.
                    var commandText = context.GetInsertIntoStagingTableCmd<T>(tmpOutputTableName, tmpTableName, context.GetTablePKs<T>().First());
                    database.ExecuteSqlCommand(commandText);

                    //Load generated IDs from temporary output table into the entities.
                    database.LoadFromTmpOutputTable(tmpOutputTableName, context.GetTablePKs<T>().First(), entityList);
                }
                else
                {
                    //Bulk inset data to temporary destination table.
                    database.BulkInsertToTable(dataTable, context.GetTableName<T>(), SqlBulkCopyOptions.Default);
                }

                affectedRows = dataTable.Rows.Count;

                //Commit if internal transaction exists.
                transaction?.Commit();
                return affectedRows;
            }
            catch (Exception)
            {
                //Rollback if internal transaction exists.
                transaction?.Rollback();
                throw;
            }
        }

        public Task<int> CommitTransactionAsync<T>(DbContext context, IEnumerable<T> collection)
        {
            throw new NotImplementedException();
        }
    }
}