using System.Linq.Expressions;
using System.Transactions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Options;

namespace TC.EntityFrameworkCore;

// 通用工作单元接口
public interface IUnitOfWork<TDbContext> where TDbContext : DbContext
{
    TDbContext Database { get; }
    IQueryable<TEntity> Q<TEntity>() where TEntity : class;
    IQueryable<TEntity> Q<TEntity>(Expression<Func<TEntity, bool>> predicate) where TEntity : class;
    void Detach<T>(T entity) where T : class;
    void Update<T>(T entity) where T : class;
    void Update<T>(IEnumerable<T> entities) where T : class;
    void Update<T>(T entity, params string[] properties) where T : class;
    void Update<T>(IEnumerable<T> entities, params string[] properties) where T : class;
    Task<int> UpdateAsync<T>(Expression<Func<T, bool>> predicate, Expression<Func<SetPropertyCalls<T>, SetPropertyCalls<T>>> property) where T : class;
    void Add<T>(T entity) where T : class;
    void AddRange<T>(IEnumerable<T> entities) where T : class;
    void Del<T>(T entity) where T : class;
    void DelRange<T>(IEnumerable<T> entities) where T : class;
    void Del<T>(IEnumerable<T> entities) where T : class;
    Task<int> DelAsync<T>(Expression<Func<T, bool>> predicate) where T : class;
    Task<int> SqlAsync(string sql, params object[] parameters);
    IQueryable<T> SqlQuery<T>(string sql);
    Task<bool> SaveChangesAsync(CancellationToken cancellationToken = default);
    bool SaveChanges();
    bool IsTracked<T>(T entity) where T : class;

    // 事务支持
    Task<IDisposable> BeginTransactionAsync(CancellationToken cancellationToken = default);
    Task CommitTransactionAsync(CancellationToken cancellationToken = default);
    Task RollbackTransactionAsync(CancellationToken cancellationToken = default);
    Task ExecuteInTransactionAsync(Func<Task> operation, CancellationToken cancellationToken = default);
    UnitOfWork<TDbContext>.AutoTransactionScope CreateLocalTransactionScope();
    UnitOfWork<TDbContext>.NestedTransactionScope CreateTransactionScope(TransactionScopeOption scopeOption = TransactionScopeOption.Required, TransactionOptions? transactionOptions = null);

    // 钩子扩展点
    event Func<object, Task>? OnBeforeSaveChanges;
    event Func<object, Task>? OnAfterSaveChanges;
}

public class UnitOfWork<TDbContext> : IUnitOfWork<TDbContext> where TDbContext : DbContext
{
    private readonly TDbContext _db;
    private readonly IOptions<TCEntityFrameworkOptions> _options;
    private IDbContextTransaction? _currentTransaction;
    private int _transactionDepth = 0;

    public TDbContext Database => _db;

    public UnitOfWork(TDbContext db,
        IOptions<TCEntityFrameworkOptions> options)
    {
        if (options == null) throw new ArgumentNullException(nameof(options));
        if (options.Value == null) throw new ArgumentNullException(nameof(options.Value));

        _db = db ?? throw new ArgumentNullException(nameof(db));
        _options = options;
    }

    public IQueryable<TEntity> Q<TEntity>() where TEntity : class
    {
        return _db.Set<TEntity>().AsQueryable().AsNoTracking();
    }

    public IQueryable<TEntity> Q<TEntity>(Expression<Func<TEntity, bool>> predicate) where TEntity : class
    {
        return _db.Set<TEntity>().AsQueryable().AsNoTracking().Where(predicate);
    }

    public void Detach<T>(T entity) where T : class
    {
        _db.Entry(entity).State = EntityState.Detached;
    }

    public void Update<T>(T entity) where T : class
    {
        if (entity is null) return;

        _db.Entry(entity).State = EntityState.Modified;
    }

    public void Update<T>(IEnumerable<T> entities) where T : class
    {
        if (entities is null || !entities.Any()) return;

        foreach (var entity in entities)
        {
            Update(entity);
        }
    }

    public void Update<T>(T entity, params string[] properties) where T : class
    {
        _db.Entry(entity).State = EntityState.Modified;
        foreach (var property in properties)
        {
            _db.Entry(entity).Property(property).IsModified = true;
        }
    }

    public void Update<T>(IEnumerable<T> entities, params string[] properties) where T : class
    {
        foreach (var entity in entities)
        {
            _db.Entry(entity).State = EntityState.Modified;
            foreach (var property in properties)
            {
                _db.Entry(entity).Property(property).IsModified = true;
            }
        }
    }

    public async Task<int> UpdateAsync<T>(
            Expression<Func<T, bool>> predicate,
            Expression<Func<SetPropertyCalls<T>, SetPropertyCalls<T>>> property
        ) where T : class
    {
        // .NET 8 的实现
        return await _db.Set<T>().Where(predicate).ExecuteUpdateAsync(property);
    }

    public void Add<T>(T entity) where T : class
    {
        if (entity is not null)
            _db.Set<T>().Add(entity);
    }

    public void AddRange<T>(IEnumerable<T> entities) where T : class
    {
        if (entities is not null && entities.Any())
            _db.Set<T>().AddRange(entities);
    }

    public void Del<T>(T entity) where T : class
    {
        if (entity is not null)
            _db.Set<T>().Remove(entity);
    }

    public void DelRange<T>(IEnumerable<T> entities) where T : class
    {
        if (entities is not null && entities.Any())
            _db.Set<T>().RemoveRange(entities);
    }

    public void Del<T>(IEnumerable<T> entities) where T : class
    {
        if (entities is not null && entities.Any())
            _db.Set<T>().RemoveRange(entities);
    }

    public async Task<int> DelAsync<T>(Expression<Func<T, bool>> predicate) where T : class
    {
        // Explicitly specify the namespace to resolve the ambiguity
        return await _db.Set<T>().Where(predicate).ExecuteDeleteAsync();
    }

    public async Task<int> SqlAsync(string sql, params object[] parameters)
    {
        return await _db.Database.ExecuteSqlRawAsync(sql, parameters);
    }

    public IQueryable<T> SqlQuery<T>(string sql)
    {
        return _db.Database.SqlQuery<T>($"{sql}");
    }

    // 钩子事件实现
    public event Func<object, Task>? OnBeforeSaveChanges;
    public event Func<object, Task>? OnAfterSaveChanges;

    #region 事务实现


    /// <summary>
    /// 开始一个事务
    /// </summary>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<IDisposable> BeginTransactionAsync(CancellationToken cancellationToken = default)
    {
        if (_currentTransaction == null)
        {
            _currentTransaction = await _db.Database.BeginTransactionAsync(cancellationToken);
            _transactionDepth = 1;
            return new SingleTransactionScope(this, _transactionDepth);
        }
        else
        {
            _transactionDepth++;
            var savepointName = GetSavepointName(_transactionDepth);
            await _currentTransaction.CreateSavepointAsync(savepointName, cancellationToken);
            return new SingleTransactionScope(this, _transactionDepth);
        }
    }

    public async Task CommitTransactionAsync(CancellationToken cancellationToken = default)
    {
        if (_currentTransaction == null)
            throw new InvalidOperationException("No active transaction.");

        if (_transactionDepth == 1)
        {
            await _db.SaveChangesAsync(cancellationToken);
            await _currentTransaction.CommitAsync(cancellationToken);
            await _currentTransaction.DisposeAsync();
            _currentTransaction = null;
            _transactionDepth = 0;
        }
        else if (_transactionDepth > 1)
        {
            var savepointName = GetSavepointName(_transactionDepth);
            await _currentTransaction.ReleaseSavepointAsync(savepointName, cancellationToken);
            _transactionDepth--;
        }
    }

    public async Task RollbackTransactionAsync(CancellationToken cancellationToken = default)
    {
        if (_currentTransaction == null)
            throw new InvalidOperationException("No active transaction.");

        if (_transactionDepth == 1)
        {
            await _currentTransaction.RollbackAsync(cancellationToken);
            await _currentTransaction.DisposeAsync();
            _currentTransaction = null;
            _transactionDepth = 0;
        }
        else if (_transactionDepth > 1)
        {
            var savepointName = GetSavepointName(_transactionDepth);
            await _currentTransaction.RollbackToSavepointAsync(savepointName, cancellationToken);
            await _currentTransaction.ReleaseSavepointAsync(savepointName, cancellationToken);
            _transactionDepth--;
        }
    }

    private static string GetSavepointName(int depth) => $"TC_UOW_SAVEPOINT_{depth}";

    /// <summary>
    /// 执行一个操作，并在操作完成后提交或回滚事务
    /// </summary>
    /// <param name="operation"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="ArgumentNullException"></exception>
    public async Task ExecuteInTransactionAsync(Func<Task> operation, CancellationToken cancellationToken = default)
    {
        if (operation == null) throw new ArgumentNullException(nameof(operation));

        using (var transaction = await BeginTransactionAsync(cancellationToken))
        {
            try
            {
                // 执行操作
                await operation();

                // 提交事务
                await CommitTransactionAsync(cancellationToken);
            }
            catch
            {
                // 回滚事务
                await RollbackTransactionAsync(cancellationToken);
                throw; // 重新抛出异常
            }
        }
    }

    /// <summary>
    /// 创建一个自动事务范围，使用完后会自动提交或回滚事务
    /// </summary>
    /// <returns></returns>
    public AutoTransactionScope CreateLocalTransactionScope()
    {
        return new AutoTransactionScope(this);
    }

    public class AutoTransactionScope : IAsyncDisposable
    {
        private readonly UnitOfWork<TDbContext> _uow;
        public AutoTransactionScope(UnitOfWork<TDbContext> uow)
        {
            _uow = uow;
        }

        public async ValueTask CommitAsync()
        {
            if (_uow._currentTransaction != null)
            {
                await _uow.CommitTransactionAsync();
                _uow._currentTransaction = null;
            }
        }

        public async ValueTask DisposeAsync()
        {
            if (_uow._currentTransaction != null)
            {
                await _uow.RollbackTransactionAsync();
                _uow._currentTransaction = null;
            }
        }
    }

    public class NestedTransactionScope : IDisposable
    {
        private readonly UnitOfWork<TDbContext> _uow;
        private readonly TransactionScope _transactionScope;
        private bool _disposed;

        public NestedTransactionScope(UnitOfWork<TDbContext> uow, TransactionScopeOption scopeOption = TransactionScopeOption.Required, TransactionOptions? transactionOptions = null)
        {
            _uow = uow ?? throw new ArgumentNullException(nameof(uow));
            _transactionScope = new TransactionScope(scopeOption, transactionOptions ?? new TransactionOptions(), TransactionScopeAsyncFlowOption.Enabled);
        }

        public void Complete()
        {
            _transactionScope.Complete();
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                _transactionScope.Dispose();
                _disposed = true;
            }
        }
    }

    /// <summary>
    /// 创建一个嵌套事务范围
    /// </summary>
    /// <param name="scopeOption"></param>
    /// <param name="transactionOptions"></param>
    /// <returns></returns>
    public NestedTransactionScope CreateTransactionScope(TransactionScopeOption scopeOption = TransactionScopeOption.Required, TransactionOptions? transactionOptions = null)
    {
        return new NestedTransactionScope(this, scopeOption, transactionOptions);
    }

    #endregion

    public Task<bool> SaveChangesAsync(CancellationToken cancellationToken = default)
    {
        if (OnBeforeSaveChanges != null)
        {
            var task = OnBeforeSaveChanges(this);
            if (task.IsCompletedSuccessfully)
            {
                return SaveChangesInternalAsync(cancellationToken);
            }
            else
            {
                return task.ContinueWith(t => SaveChangesInternalAsync(cancellationToken).Result, cancellationToken);
            }
        }
        else
        {
            return SaveChangesInternalAsync(cancellationToken);
        }
    }

    private async Task<bool> SaveChangesInternalAsync(CancellationToken cancellationToken = default)
    {
        var result = await _db.SaveChangesAsync(cancellationToken);
        if (OnAfterSaveChanges != null)
        {
            await OnAfterSaveChanges(this);
        }
        return result > 0;
    }

    public bool SaveChanges()
    {
        if (OnBeforeSaveChanges != null)
        {
            OnBeforeSaveChanges(this).GetAwaiter().GetResult();
        }
        var result = _db.SaveChanges();
        if (OnAfterSaveChanges != null)
        {
            OnAfterSaveChanges(this).GetAwaiter().GetResult();
        }
        return result > 0;
    }

    public bool IsTracked<T>(T entity) where T : class
    {
        return _db.Entry(entity).State != EntityState.Detached;
    }

    private class SingleTransactionScope : IDisposable
    {
        private readonly UnitOfWork<TDbContext> _uow;
        private readonly int _scopeDepth;
        private bool _disposed = false;

        public SingleTransactionScope(UnitOfWork<TDbContext> uow, int scopeDepth)
        {
            _uow = uow;
            _scopeDepth = scopeDepth;
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                // 自动减少深度，防止嵌套泄漏
                if (_uow._transactionDepth == _scopeDepth)
                {
                    _uow._transactionDepth--;
                    if (_uow._transactionDepth == 0 && _uow._currentTransaction != null)
                    {
                        _uow._currentTransaction.Dispose();
                        _uow._currentTransaction = null;
                    }
                }
                _disposed = true;
            }
        }
    }
}