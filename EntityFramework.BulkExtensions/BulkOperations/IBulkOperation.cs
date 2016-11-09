using System.Collections.Generic;
using System.Data.Entity;
using System.Threading.Tasks;
using EntityFramework.BulkExtensions.Operations;

// ReSharper disable once CheckNamespace

namespace SqlBulkTools
{
    internal interface IBulkOperation
    {
        int CommitTransaction<T>(DbContext context, IEnumerable<T> collection, Identity identity = Identity.InputOnly) where T : class;
        Task<int> CommitTransactionAsync<T>(DbContext context, IEnumerable<T> collection);
    }
}