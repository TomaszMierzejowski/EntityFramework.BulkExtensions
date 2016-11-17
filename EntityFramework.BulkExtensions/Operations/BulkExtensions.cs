using System.Collections.Generic;
using System.Data.Entity;
using EntityFramework.BulkExtensions.BulkOperations;

namespace EntityFramework.BulkExtensions.Operations
{
    /// <summary>
    /// 
    /// </summary>
    public static class BulkExtensions
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="context"></param>
        /// <param name="entities"></param>
        /// <param name="identity"></param>
        /// <typeparam name="TEntity"></typeparam>
        /// <returns></returns>
        public static int BulkInsert<TEntity>(this DbContext context, IEnumerable<TEntity> entities, Identity identity = Identity.InputOnly) where TEntity : class
        {
            return OperationFactory.BulkInsert.CommitTransaction(context, entities, identity);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="context"></param>
        /// <param name="entities"></param>
        /// <typeparam name="TEntity"></typeparam>
        /// <returns></returns>
        public static int BulkUpdate<TEntity>(this DbContext context, IEnumerable<TEntity> entities) where TEntity : class
        {
            return OperationFactory.BulkUpdate.CommitTransaction(context, entities);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="context"></param>
        /// <param name="entities"></param>
        /// <typeparam name="TEntity"></typeparam>
        /// <returns></returns>
        public static int BulkDelete<TEntity>(this DbContext context, IEnumerable<TEntity> entities) where TEntity : class
        {
            return OperationFactory.BulkDelete.CommitTransaction(context, entities);
        }
    }

    public enum Identity
    {
        InputOnly, InputOutput
    }
}