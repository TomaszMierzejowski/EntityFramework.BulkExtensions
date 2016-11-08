using System;
using System.Collections.Generic;
using System.Data.Entity;
using EntityFramework.BulkExtensions.BulkOperations;
using EntityFramework.BulkExtensions.Helpers;

namespace EntityFramework.BulkExtensions.Extensions
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
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public static int BulkInsert<T>(this DbContext context, IEnumerable<T> entities, Identity identity = Identity.InputOnly) where T : class
        {
            return new BulkInsert().CommitTransaction(context, entities, identity);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="context"></param>
        /// <param name="entities"></param>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public static int BulkUpdate<T>(this DbContext context, IEnumerable<T> entities) where T : class
        {
            return new BulkUpdate().CommitTransaction(context, entities);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="context"></param>
        /// <param name="entities"></param>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        public static int BulkDelete<T>(this DbContext context, IEnumerable<T> entities) where T : class
        {
            throw new NotImplementedException();
        }
    }
}