using System;
using System.Data.Common;
using System.Diagnostics;
using System.Linq.Expressions;
using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace TC.EntityFrameworkCore;

public class TCEntityFrameworkOptions : IOptions<TCEntityFrameworkOptions>
{
    public TCEntityFrameworkOptions Value => this;

    const string PREFIX = "[TCEntityFramework]: ";

    public Action<string> Logger { get; set; } = message => Debug.WriteLine($"{PREFIX}{message}");
}

public static class TCEntityFrameworkExtensions
{
    private static TCEntityFrameworkOptions? _options;

    public static TCEntityFrameworkOptions Options
    {
        get => _options ??= new TCEntityFrameworkOptions();
    }

    public static void AddTCEntityFramework<TDbContext>(this IServiceCollection services,
        Action<TCEntityFrameworkOptions> configureOptions)
    where TDbContext : DbContext
    {
        if (configureOptions == null) throw new ArgumentNullException(nameof(configureOptions));

        var options = new TCEntityFrameworkOptions();
        configureOptions(options);

        services.Configure<TCEntityFrameworkOptions>(opts =>
        {
            opts.Logger = options.Logger;
        });

        // 注册接口和实现，便于扩展和替换
        services.AddScoped<IUnitOfWork<TDbContext>, UnitOfWork<TDbContext>>();
    }


    public static DbContextOptionsBuilder AddTrace(this DbContextOptionsBuilder builder, int slowThresholdMs = 500)
    {
        if (builder == null) throw new ArgumentNullException(nameof(builder));
        builder.AddInterceptors(new SqlTraceInterceptor(Options.Logger, slowThresholdMs));
        return builder;
    }

    public static IQueryable<T> WhereIf<T>(this IQueryable<T> query, bool condition, Expression<Func<T, bool>> predicate)
    {
        return condition ? query.Where(predicate) : query;
    }

    public static IQueryable<T> OrderBy<T>(this IQueryable<T> query, string propertyName, bool desc = false)
    {
        if (string.IsNullOrWhiteSpace(propertyName)) return query;
        var param = Expression.Parameter(typeof(T), "x");
        var body = Expression.PropertyOrField(param, propertyName);
        var lambda = Expression.Lambda(body, param);
        string method = desc ? "OrderByDescending" : "OrderBy";
        var types = new Type[] { typeof(T), body.Type };
        var mce = Expression.Call(typeof(Queryable), method, types, query.Expression, Expression.Quote(lambda));
        return query.Provider.CreateQuery<T>(mce);
    }

    public static IQueryable<T> PageBy<T>(this IQueryable<T> query, int pageIndex, int pageSize)
    {
        return query.Skip((pageIndex - 1) * pageSize).Take(pageSize);
    }
}


public class SqlTraceInterceptor : DbCommandInterceptor
{
    private readonly Action<string> _logger;
    private readonly int _slowThresholdMs;

    public SqlTraceInterceptor(Action<string> logger, int slowThresholdMs = 500)
    {
        _logger = logger;
        _slowThresholdMs = slowThresholdMs;
    }

    private void LogCommand(DbCommand command, long elapsedMs = 0)
    {
        var sb = new StringBuilder();
        sb.AppendLine("SQL: " + command.CommandText);
        if (command.Parameters.Count > 0)
        {
            sb.AppendLine("Parameters:");
            foreach (DbParameter param in command.Parameters)
            {
                sb.AppendLine($"  {param.ParameterName} = {param.Value} ({param.DbType})");
            }
        }
        if (elapsedMs > 0)
        {
            sb.AppendLine($"Elapsed: {elapsedMs}ms");
            if (elapsedMs >= _slowThresholdMs)
                sb.AppendLine("!!! SLOW QUERY !!!");
        }
        _logger(sb.ToString());
    }

    public override InterceptionResult<DbDataReader> ReaderExecuting(
        DbCommand command, CommandEventData eventData, InterceptionResult<DbDataReader> result)
    {
        var sw = Stopwatch.StartNew();
        var res = base.ReaderExecuting(command, eventData, result);
        sw.Stop();
        LogCommand(command, sw.ElapsedMilliseconds);
        return res;
    }

    public override InterceptionResult<int> NonQueryExecuting(
        DbCommand command, CommandEventData eventData, InterceptionResult<int> result)
    {
        var sw = Stopwatch.StartNew();
        var res = base.NonQueryExecuting(command, eventData, result);
        sw.Stop();
        LogCommand(command, sw.ElapsedMilliseconds);
        return res;
    }

    public override InterceptionResult<object> ScalarExecuting(
        DbCommand command, CommandEventData eventData, InterceptionResult<object> result)
    {
        var sw = Stopwatch.StartNew();
        var res = base.ScalarExecuting(command, eventData, result);
        sw.Stop();
        LogCommand(command, sw.ElapsedMilliseconds);
        return res;
    }
}
