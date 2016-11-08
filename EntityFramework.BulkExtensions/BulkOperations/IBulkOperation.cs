using System.Collections.Generic;
using System.Data.Entity;
using System.Data.SqlClient;
using System.Threading.Tasks;
using EntityFramework.BulkExtensions.Helpers;

// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    internal interface IBulkOperation
    {
        int CommitTransaction<T>(DbContext context, IEnumerable<T> collection, Identity identity) where T : class;
        Task<int> CommitTransactionAsync<T>(DbContext context, IEnumerable<T> collection);
    }
}
