//-----------------------------------------------------------------------
// <copyright file="DocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Document.Batches;
using Raven.NewClient.Client.Indexes;
using Raven.NewClient.Client.Linq;
using System.Linq;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Commands.Lazy;

namespace Raven.NewClient.Client.Document
{
    /// <summary>
    /// Implements Unit of Work for accessing the RavenDB server
    /// </summary>
    public partial class DocumentSession : InMemoryDocumentSessionOperations, IDocumentQueryGenerator, ISyncAdvancedSessionOperation, IDocumentSessionImpl
    {
        /// <summary>
        /// Begin a load while including the specified path 
        /// </summary>
        /// <param name="path">The path.</param>
        ILazyLoaderWithInclude<T> ILazySessionOperations.Include<T>(Expression<Func<T, object>> path)
        {
            return new LazyMultiLoaderWithInclude<T>(this).Include(path);
        }

        /// <summary>
        /// Loads the specified ids.
        /// </summary>
        Lazy<Dictionary<string, T>> ILazySessionOperations.Load<T>(IEnumerable<string> ids)
        {
            return Lazily.Load<T>(ids, null);
        }

        /// <summary>
        /// Loads the specified ids and a function to call when it is evaluated
        /// </summary>
        Lazy<Dictionary<string, T>> ILazySessionOperations.Load<T>(IEnumerable<string> ids, Action<Dictionary<string, T>> onEval)
        {
            return LazyLoadInternal(ids.ToArray(), new string[0], onEval);
        }

        /// <summary>
        /// Loads the specified id.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id">The id.</param>
        /// <returns></returns>
        Lazy<T> ILazySessionOperations.Load<T>(string id)
        {
            return Lazily.Load(id, (Action<T>)null);
        }

        /// <summary>
        /// Loads the specified id and a function to call when it is evaluated
        /// </summary>
        Lazy<T> ILazySessionOperations.Load<T>(string id, Action<T> onEval)
        {
            if (IsLoaded(id))
                return new Lazy<T>(() => Load<T>(id));
            //TODO - DisableAllCaching
            var lazyLoadOperation = new LazyLoadOperation<T>(new LoadOperation(this, new []{id}), id);
            return AddLazyOperation(lazyLoadOperation, onEval);
        }

        internal Lazy<T> AddLazyOperation<T>(ILazyOperation operation, Action<T> onEval)
        {
            pendingLazyOperations.Add(operation);
            var lazyValue = new Lazy<T>(() =>
            {
                ExecuteAllPendingLazyOperations();
                return GetOperationResult<T>(operation.Result);
            });

            if (onEval != null)
                onEvaluateLazy[operation] = theResult => onEval(GetOperationResult<T>(theResult));

            return lazyValue;
        }

        /// <summary>
        /// Loads the specified entities with the specified id after applying
        /// conventions on the provided id to get the real document id.
        /// </summary>
        /// <remarks>
        /// This method allows you to call:
        /// Load{Post}(1)
        /// And that call will internally be translated to 
        /// Load{Post}("posts/1");
        /// 
        /// Or whatever your conventions specify.
        /// </remarks>
        Lazy<T> ILazySessionOperations.Load<T>(ValueType id)
        {
            return Lazily.Load(id, (Action<T>)null);
        }

        /// <summary>
        /// Loads the specified entities with the specified id after applying
        /// conventions on the provided id to get the real document id.
        /// </summary>
        /// <remarks>
        /// This method allows you to call:
        /// Load{Post}(1)
        /// And that call will internally be translated to 
        /// Load{Post}("posts/1");
        /// 
        /// Or whatever your conventions specify.
        /// </remarks>
        Lazy<T> ILazySessionOperations.Load<T>(ValueType id, Action<T> onEval)
        {
            var documentKey = Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false);
            return Lazily.Load(documentKey, onEval);
        }

        Lazy<Dictionary<string, T>> ILazySessionOperations.Load<T>(params ValueType[] ids)
        {
            var documentKeys = ids.Select(id => Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false));
            return Lazily.Load<T>(documentKeys);
        }

        Lazy<Dictionary<string, T>> ILazySessionOperations.Load<T>(IEnumerable<ValueType> ids)
        {
            var documentKeys = ids.Select(id => Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false));
            return Lazily.Load<T>(documentKeys);
        }

        Lazy<Dictionary<string, T>> ILazySessionOperations.Load<T>(IEnumerable<ValueType> ids, Action<IDictionary<string, T>> onEval)
        {
            var documentKeys = ids.Select(id => Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false));
            return LazyLoadInternal(documentKeys.ToArray(), new string[0], onEval);
        }

        Lazy<TResult> ILazySessionOperations.Load<TTransformer, TResult>(string id, Action<ILoadConfiguration> configure,
            Action<TResult> onEval)
        {
            var transformer = new TTransformer().TransformerName;
            var ids = new[] {id};
            var configuration = new RavenLoadConfiguration();
            configure?.Invoke(configuration);

            var lazyLoadOperation = new LazyTransformerLoadOperation<TResult>(
                ids,
                transformer,
                configuration.TransformerParameters,
                new LoadTransformerOperation(this),
                singleResult: true);

            return AddLazyOperation(lazyLoadOperation, onEval);
        }

        Lazy<TResult> ILazySessionOperations.Load<TResult>(string id, Type transformerType, Action<ILoadConfiguration> configure, Action<TResult> onEval)
        {
            var transformer = ((AbstractTransformerCreationTask)Activator.CreateInstance(transformerType)).TransformerName;
            var ids = new[] { id };
            var configuration = new RavenLoadConfiguration();
            if (configure != null)
                configure(configuration);

            var lazyLoadOperation = new LazyTransformerLoadOperation<TResult>(
                ids,
                transformer,
                configuration.TransformerParameters,
                new LoadTransformerOperation(this),
                singleResult: true);

            return AddLazyOperation(lazyLoadOperation, onEval);
        }

        Lazy<TResult[]> ILazySessionOperations.Load<TTransformer, TResult>(IEnumerable<string> ids, Action<ILoadConfiguration> configure, Action<TResult> onEval)
        {
            return Lazily.Load(ids, typeof(TTransformer), configure, onEval);
        }

        Lazy<TResult[]> ILazySessionOperations.Load<TResult>(IEnumerable<string> ids, Type transformerType, Action<ILoadConfiguration> configure, Action<TResult> onEval)
        {
            var transformer = ((AbstractTransformerCreationTask)Activator.CreateInstance(transformerType)).TransformerName;

            var configuration = new RavenLoadConfiguration();
            if (configure != null)
                configure(configuration);

            var idsArray = ids.ToArray();

            var lazyLoadOperation = new LazyTransformerLoadOperation<TResult>(
                idsArray,
                transformer,
                configuration.TransformerParameters,
                new LoadTransformerOperation(this),
                singleResult: false);

            return AddLazyOperation<TResult[]>(lazyLoadOperation, null);
        }

        Lazy<IDictionary<string, T>> ILazySessionOperations.LoadStartingWith<T>(string keyPrefix, string matches, int start, int pageSize, string exclude, RavenPagingInformation pagingInformation, string skipAfter)
        {
            var operation = new LazyStartsWithOperation<T>(keyPrefix, matches, exclude, start, pageSize, this, pagingInformation, skipAfter);

            return AddLazyOperation<IDictionary<string, T>>(operation, null);
        }


        public Lazy<Dictionary<string, TResult>> MoreLikeThis<TResult>(MoreLikeThisQuery query)
        {
            //TODO - DisableAllCaching
            var loadOperation = new LoadOperation(this);
            var lazyOp = new LazyMoreLikeThisOperation<TResult>(loadOperation, query);
            return AddLazyOperation<Dictionary<string, TResult>>(lazyOp, null);
        }

        /// <summary>
        /// Begin a load while including the specified path 
        /// </summary>
        /// <param name="path">The path.</param>
        ILazyLoaderWithInclude<object> ILazySessionOperations.Include(string path)
        {
            return new LazyMultiLoaderWithInclude<object>(this).Include(path);
        }

        /// <summary>
        /// Register to lazily load documents and include
        /// </summary>
        public Lazy<Dictionary<string, T>> LazyLoadInternal<T>(string[] ids, string[] includes, Action<Dictionary<string, T>> onEval)
        {
            if (CheckIfIdAlreadyIncluded(ids, includes))
            {
                return new Lazy<Dictionary<string, T>>(() => ids.ToDictionary(x => x, Load<T>));
            }
            var loadOperation = new LoadOperation(this, ids, includes);
            var lazyOp = new LazyLoadOperation<T>(loadOperation, ids, includes);
            return AddLazyOperation(lazyOp, onEval);
        }

    }
}