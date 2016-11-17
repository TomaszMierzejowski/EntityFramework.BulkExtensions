using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Data.Entity.Core;
using System.Data.SqlClient;
using System.Linq;
using EntityFramework.BulkExtensions.Extensions;
using EntityFramework.BulkExtensions.Helpers;
using EntityFramework.BulkExtensions.Operations;

namespace EntityFramework.BulkExtensions.BulkOperations
{
    public class BulkInsert : IBulkOperation
    {
        int IBulkOperation.CommitTransaction<TEntity>(DbContext context, IEnumerable<TEntity> collection, Identity identity)
        {
            if (!context.Exists<TEntity>())
            {
                throw new EntityException(@"Entity is not being mapped by Entity Framework.Check your model.");
            }
            var tmpTableName = context.RandomTableName<TEntity>();
            var entityList = collection.ToList();
            var database = context.Database;
            var affectedRows = 0;
            if (!entityList.Any())
            {
                return affectedRows;
            }

            //Creates inner transaction for the scope of the operation if the context doens't have one.
            var transaction = context.InternalTransaction();
            try
            {
                //Cconvert entity collection into a DataTable
                var dataTable = context.ToDataTable(entityList);

                //Return generated IDs for bulk inserted elements.
                if (identity == Identity.InputOutput)
                {
                    //Create temporary table.
                    var command = context.BuildCreateTempTable<TEntity>(tmpTableName);
                    database.ExecuteSqlCommand(command);

                    //Bulk inset data to temporary temporary table.
                    database.BulkInsertToTable(dataTable, tmpTableName, SqlBulkCopyOptions.Default);

                    var tmpOutputTableName = context.RandomTableName<TEntity>();
                    //Copy data from temporary table to destination table with ID output to another temporary table.
                    var commandText = context.GetInsertIntoStagingTableCmd<TEntity>(tmpOutputTableName, tmpTableName, context.GetTablePKs<TEntity>().First());
                    database.ExecuteSqlCommand(commandText);

                    //Load generated IDs from temporary output table into the entities.
                    database.LoadFromTmpOutputTable(tmpOutputTableName, context.GetTablePKs<TEntity>().First(), entityList);
                }
                else
                {
                    //Bulk inset data to temporary destination table.
                    database.BulkInsertToTable(dataTable, context.GetTableName<TEntity>(), SqlBulkCopyOptions.Default);
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
    }
}