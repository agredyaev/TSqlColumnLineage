using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using TSqlColumnLineage.Core.Analysis.Handlers.Base;
using TSqlColumnLineage.Core.Analysis.Visitors.Base;
using TSqlColumnLineage.Core.Common.Logging;
using TSqlColumnLineage.Core.Common.Utils;

namespace TSqlColumnLineage.Core.Analysis.Handlers
{
    /// <summary>
    /// Maps SQL fragment types to appropriate handlers automatically
    /// </summary>
    public sealed class HandlerMapper
    {
        private readonly Dictionary<Type, List<Type>> _handlerTypeMap = new Dictionary<Type, List<Type>>();
        private readonly ILogger _logger;
        
        /// <summary>
        /// Creates a new handler mapper
        /// </summary>
        /// <param name="logger">Logger for diagnostic information</param>
        public HandlerMapper(ILogger logger = null)
        {
            _logger = logger;
        }
        
        /// <summary>
        /// Scans an assembly for handler implementations and builds a map of handlers
        /// </summary>
        /// <param name="assembly">Assembly to scan (default: current assembly)</param>
        public void ScanAssembly(Assembly assembly = null)
        {
            assembly = assembly ?? Assembly.GetExecutingAssembly();
            
            _logger?.LogInformation($"Scanning assembly {assembly.GetName().Name} for handlers");
            
            // Find all handler types
            var handlerTypes = assembly.GetTypes()
                .Where(t => !t.IsAbstract && !t.IsInterface && typeof(IQueryHandler).IsAssignableFrom(t))
                .ToList();
                
            _logger?.LogDebug($"Found {handlerTypes.Count} handler types");
            
            // Analyze each handler type to determine what it can handle
            foreach (var handlerType in handlerTypes)
            {
                AnalyzeHandlerType(handlerType);
            }
            
            _logger?.LogInformation($"Mapped {_handlerTypeMap.Count} fragment types to handlers");
        }
        
        /// <summary>
        /// Analyzes a handler type to determine what fragments it can handle
        /// </summary>
        private void AnalyzeHandlerType(Type handlerType)
        {
            try
            {
                // Look for a CanHandle method implementation
                var canHandleMethod = handlerType.GetMethod("CanHandle", BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly);
                if (canHandleMethod == null)
                {
                    _logger?.LogWarning($"Handler {handlerType.Name} does not implement CanHandle method");
                    return;
                }
                
                // Look for explicit type checks in the CanHandle method
                var fragments = GetHandledFragmentTypes(handlerType);
                
                foreach (var fragmentType in fragments)
                {
                    if (!_handlerTypeMap.TryGetValue(fragmentType, out var handlers))
                    {
                        handlers = new List<Type>();
                        _handlerTypeMap[fragmentType] = handlers;
                    }
                    
                    handlers.Add(handlerType);
                    _logger?.LogDebug($"Mapped {fragmentType.Name} to {handlerType.Name}");
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, $"Error analyzing handler type {handlerType.Name}");
            }
        }
        
        /// <summary>
        /// Gets the fragment types a handler can handle through reflection with improved error handling
        /// </summary>
        private List<Type> GetHandledFragmentTypes(Type handlerType)
        {
            var result = new List<Type>();
            
            try
            {
                // Method 1: Check for [HandlesFragment] attributes
                var attributes = handlerType.GetCustomAttributes<HandlesFragmentAttribute>(false);
                if (attributes != null)
                {
                    foreach (var attr in attributes)
                    {
                        if (attr.FragmentType != null && !result.Contains(attr.FragmentType))
                        {
                            result.Add(attr.FragmentType);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger?.LogWarning($"Error getting attributes for handler {handlerType.Name}: {ex.Message}");
            }
            
            try
            {
                // Method 2: Check for explicit overrides of ExplicitVisit methods
                var visitMethods = handlerType.GetMethods()
                    .Where(m => m.Name.StartsWith("ExplicitVisit") && 
                            m.GetParameters().Length == 1 &&
                            m.GetParameters()[0].ParameterType.IsSubclassOf(typeof(TSqlFragment)))
                    .ToList();
                    
                foreach (var method in visitMethods)
                {
                    try
                    {
                        var fragmentType = method.GetParameters()[0].ParameterType;
                        if (fragmentType != null && !result.Contains(fragmentType))
                        {
                            result.Add(fragmentType);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger?.LogWarning($"Error processing method {method.Name} on handler {handlerType.Name}: {ex.Message}");
                    }
                }
            }
            catch (Exception ex)
            {
                _logger?.LogWarning($"Error getting methods for handler {handlerType.Name}: {ex.Message}");
            }
            
            try
            {
                // Method 3: Check for implemented interfaces
                var interfaces = handlerType.GetInterfaces()
                    .Where(i => i.IsGenericType && 
                            i.GetGenericTypeDefinition() == typeof(IQueryHandler<>))
                    .ToList();
                    
                foreach (var iface in interfaces)
                {
                    try
                    {
                        var fragmentType = iface.GetGenericArguments()[0];
                        if (fragmentType != null && !result.Contains(fragmentType))
                        {
                            result.Add(fragmentType);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger?.LogWarning($"Error processing interface {iface.Name} on handler {handlerType.Name}: {ex.Message}");
                    }
                }
            }
            catch (Exception ex)
            {
                _logger?.LogWarning($"Error getting interfaces for handler {handlerType.Name}: {ex.Message}");
            }
            
            // Method 4: Check for implementation of CanHandle method
            try
            {
                var canHandleMethod = handlerType.GetMethod("CanHandle", 
                    BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly);
                    
                if (canHandleMethod != null)
                {
                    // If we found a CanHandle method but couldn't determine types, add TSqlFragment as a fallback
                    if (result.Count == 0)
                    {
                        result.Add(typeof(TSqlFragment));
                    }
                }
            }
            catch (Exception ex)
            {
                _logger?.LogWarning($"Error checking CanHandle method on handler {handlerType.Name}: {ex.Message}");
            }
            
            return result;
        }
        
        /// <summary>
        /// Creates and configures a handler registry based on the mapped handlers
        /// </summary>
        /// <param name="context">Visitor context</param>
        /// <param name="stringPool">String pool</param>
        /// <param name="idGenerator">ID generator</param>
        /// <param name="logger">Logger</param>
        /// <returns>Configured handler registry</returns>
        public HandlerRegistry CreateHandlerRegistry(
            VisitorContext context,
            StringPool stringPool,
            IdGenerator idGenerator,
            ILogger logger = null)
        {
            var registry = new HandlerRegistry(stringPool);
            
            // Create handler instances for each registered handler type
            foreach (var handlers in _handlerTypeMap.Values)
            {
                foreach (var handlerType in handlers)
                {
                    try
                    {
                        // Find an appropriate constructor
                        var constructor = FindSuitableConstructor(handlerType);
                        if (constructor == null)
                        {
                            _logger?.LogWarning($"No suitable constructor found for {handlerType.Name}");
                            continue;
                        }
                        
                        // Create constructor parameters
                        var parameters = CreateConstructorParameters(constructor, context, stringPool, idGenerator, logger);
                        
                        // Create handler instance
                        var handler = constructor.Invoke(parameters) as IQueryHandler;
                        if (handler != null)
                        {
                            registry.RegisterHandler(handler);
                            _logger?.LogDebug($"Created and registered handler: {handlerType.Name}");
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger?.LogError(ex, $"Error creating handler {handlerType.Name}");
                    }
                }
            }
            
            return registry;
        }
        
        /// <summary>
        /// Finds a suitable constructor for a handler type
        /// </summary>
        private ConstructorInfo FindSuitableConstructor(Type handlerType)
        {
            // Get all public constructors
            var constructors = handlerType.GetConstructors()
                .OrderByDescending(c => c.GetParameters().Length)  // Prefer constructor with most parameters
                .ToList();
                
            foreach (var ctor in constructors)
            {
                var parameters = ctor.GetParameters();
                
                // Check if we can satisfy all parameters
                bool canSatisfy = true;
                foreach (var param in parameters)
                {
                    if (param.ParameterType == typeof(VisitorContext) ||
                        param.ParameterType == typeof(StringPool) ||
                        param.ParameterType == typeof(IdGenerator) ||
                        param.ParameterType == typeof(ILogger) ||
                        param.IsOptional)
                    {
                        // We can satisfy this parameter
                        continue;
                    }
                    
                    // We can't satisfy this parameter
                    canSatisfy = false;
                    break;
                }
                
                if (canSatisfy)
                {
                    return ctor;
                }
            }
            
            return null;
        }
        
        /// <summary>
        /// Creates parameters for a constructor
        /// </summary>
        private object[] CreateConstructorParameters(
            ConstructorInfo constructor,
            VisitorContext context,
            StringPool stringPool,
            IdGenerator idGenerator,
            ILogger logger)
        {
            var parameters = constructor.GetParameters();
            var args = new object[parameters.Length];
            
            for (int i = 0; i < parameters.Length; i++)
            {
                var param = parameters[i];
                
                if (param.ParameterType == typeof(VisitorContext))
                {
                    args[i] = context;
                }
                else if (param.ParameterType == typeof(StringPool))
                {
                    args[i] = stringPool;
                }
                else if (param.ParameterType == typeof(IdGenerator))
                {
                    args[i] = idGenerator;
                }
                else if (param.ParameterType == typeof(ILogger))
                {
                    args[i] = logger;
                }
                else if (param.IsOptional)
                {
                    args[i] = param.DefaultValue;
                }
                else
                {
                    args[i] = null;
                }
            }
            
            return args;
        }
    }
    
    /// <summary>
    /// Attribute to mark handlers with the fragment types they can handle
    /// </summary>
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = true)]
    public class HandlesFragmentAttribute : Attribute
    {
        /// <summary>
        /// Fragment type this handler can handle
        /// </summary>
        public Type FragmentType { get; }
        
        /// <summary>
        /// Creates a new attribute
        /// </summary>
        /// <param name="fragmentType">Fragment type this handler can handle</param>
        public HandlesFragmentAttribute(Type fragmentType)
        {
            FragmentType = fragmentType ?? throw new ArgumentNullException(nameof(fragmentType));
            
            if (!fragmentType.IsSubclassOf(typeof(TSqlFragment)))
            {
                throw new ArgumentException($"Type {fragmentType.Name} must be a subclass of TSqlFragment");
            }
        }
    }
    
    /// <summary>
    /// Interface for strongly-typed query handlers
    /// </summary>
    /// <typeparam name="T">Fragment type to handle</typeparam>
    public interface IQueryHandler<T> : IQueryHandler where T : TSqlFragment
    {
        /// <summary>
        /// Handles a specific fragment type
        /// </summary>
        /// <param name="fragment">Fragment to handle</param>
        /// <param name="context">Visitor context</param>
        /// <returns>True if fully handled</returns>
        bool Handle(T fragment, VisitorContext context);
    }
}