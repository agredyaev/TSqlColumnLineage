using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using TSqlColumnLineage.Core.Engine.Parsing.Models;
using TSqlColumnLineage.Core.Infrastructure;
using TSqlColumnLineage.Core.Infrastructure.Memory;
using TSqlColumnLineage.Core.Infrastructure.Monitoring;

namespace TSqlColumnLineage.Core.Engine.Parsing
{
    /// <summary>
    /// High-performance SQL parser service for TSqlColumnLineage.
    /// Utilizes data-oriented design and memory optimization for processing large scripts.
    /// </summary>
    public class SqlParser
    {
        // Default parsing options
        private static readonly ParsingOptions _defaultOptions = new();

        // Performance tracking
        private readonly PerformanceTracker _performanceTracker;

        // Memory manager for optimizations
        private readonly MemoryManager _memoryManager;

        /// <summary>
        /// Creates a new SqlParser
        /// </summary>
        public SqlParser()
        {
            // Get infrastructure services
            _performanceTracker = PerformanceTracker.Instance;
            _memoryManager = MemoryManager.Instance;
        }

        /// <summary>
        /// Parses a T-SQL script synchronously
        /// </summary>
        public ParsedScript Parse(string scriptText, string source = "", ParsingOptions options = null)
        {
            options ??= _defaultOptions;

            // Track performance
            using var perfTracker = _performanceTracker.TrackOperation("Parsing", "ParseScript");

            // Initialize parser and token stream
            TSqlParser parser = null;
            IList<TSqlParserToken> tokenStream = null;

            try
            {
                parser = ParserFactory.CreateParser(options);
                tokenStream = ParserFactory.CreateTokenStream();

                // Parse the script
                TSqlFragment scriptAst;
                IList<ParseError> errors;

                using (var reader = new StringReader(scriptText))
                {
                    scriptAst = parser.Parse(reader, out errors);
                }

                if (scriptAst == null)
                {
                    return CreateErrorResult(scriptText, source, errors.ToList(), options);
                }

                // Process the parsed script
                return ProcessParsedScript(scriptAst, null, scriptText, source, errors.ToList(), options);
            }
            catch (Exception ex)
            {
                _performanceTracker.IncrementCounter("Parsing", "Errors");
                _performanceTracker.IncrementCounter("Parsing", "ExceptionErrors");

                var error = new ParseError
                {
                    Message = ex.Message,
                    ErrorCode = -1,
                    Line = 0,
                    Column = 0
                };

                return CreateErrorResult(scriptText, source, new List<ParseError> { error }, options);
            }
            finally
            {
                // Return parser to pool
                if (parser != null)
                {
                    ParserFactory.ReturnParser(parser);
                }
            }
        }

        /// <summary>
        /// Parses a T-SQL script asynchronously
        /// </summary>
        public async Task<ParsedScript> ParseAsync(string scriptText, string source = "", ParsingOptions options = null, CancellationToken cancellationToken = default)
        {
            options ??= _defaultOptions;

            // Track performance
            using var perfTracker = _performanceTracker.TrackOperation("Parsing", "ParseScriptAsync");

            try
            {
                // Perform parsing on a background thread
                return await Task.Run(() => Parse(scriptText, source, options), cancellationToken);
            }
            catch (OperationCanceledException)
            {
                _performanceTracker.IncrementCounter("Parsing", "Cancelled");
                throw;
            }
            catch (Exception ex)
            {
                _performanceTracker.IncrementCounter("Parsing", "Errors");
                _performanceTracker.IncrementCounter("Parsing", "ExceptionErrors");

                var error = new ParseError
                {
                    Message = ex.Message,
                    ErrorCode = -1,
                    Line = 0,
                    Column = 0
                };

                return CreateErrorResult(scriptText, source, new List<ParseError> { error }, options);
            }
        }

        /// <summary>
        /// Parses a large T-SQL script with streaming to minimize memory usage
        /// </summary>
        public async Task<ParsedScript> ParseStreamingAsync(string scriptText, string source = "", ParsingOptions options = null, CancellationToken cancellationToken = default)
        {
            options ??= _defaultOptions;

            // Track performance
            using var perfTracker = _performanceTracker.TrackOperation("Parsing", "ParseStreamingAsync");

            return await Task.Run(() => ParseStreaming(scriptText, source, options, cancellationToken), cancellationToken);
        }

        /// <summary>
        /// Parses a large T-SQL script with streaming to minimize memory usage
        /// </summary>
        private ParsedScript ParseStreaming(string scriptText, string source, ParsingOptions options, CancellationToken cancellationToken)
        {
            // Performance counter for fragments
            int fragmentsProcessed = 0;

            // Initialize parser and token stream
            TSqlParser parser = null;
            IList<TSqlParserToken> tokenStream = null;
            var errors = new List<ParseError>();
            var batches = new List<SqlFragment>();

            try
            {
                parser = ParserFactory.CreateParser(options);
                tokenStream = ParserFactory.CreateTokenStream();

                // Parse the script and process batches efficiently
                using (var reader = new StringReader(scriptText))
                {
                    // Get token stream
                    IList<ParseError> tokenErrors;
                    parser.GetTokenStream(reader, tokenStream, out tokenErrors);
                    errors.AddRange(tokenErrors);

                    // Split script into batches based on GO separator
                    var batchSql = new StringBuilder();
                    int batchStartOffset = 0;
                    int batchLineNumber = 1;
                    int batchColumnNumber = 1;

                    void ProcessAccumulatedBatch()
                    {
                        if (batchSql.Length == 0) return;

                        // Parse this batch
                        string batchText = batchSql.ToString();
                        TSqlFragment batchAst;
                        IList<ParseError> batchErrors;

                        using (var batchReader = new StringReader(batchText))
                        {
                            batchAst = parser.Parse(batchReader, out batchErrors);
                        }

                        // Adjust error positions
                        foreach (var error in batchErrors)
                        {
                            error.Line += batchLineNumber - 1;
                            error.StartOffset += batchStartOffset;
                            error.EndOffset += batchStartOffset;
                        }
                        errors.AddRange(batchErrors);

                        // Process batch if parsing succeeded
                        if (batchAst != null)
                        {
                            // Process batch and track fragment count
                            var batchFragment = ProcessBatch(batchAst, batchText, batchStartOffset, batchLineNumber, batchColumnNumber, options);
                            batches.Add(batchFragment);
                            fragmentsProcessed += batchFragment.Children.Count + 1; // +1 for batch itself
                        }

                        // Clear the buffer
                        batchSql.Clear();
                    }

                    // Process tokens to find batch separators
                    foreach (var token in tokenStream)
                    {
                        // Check for cancellation
                        cancellationToken.ThrowIfCancellationRequested();

                        if (token.TokenType == TSqlTokenType.Go)
                        {
                            // Process accumulated batch
                            ProcessAccumulatedBatch();

                            // Update batch start position
                            batchStartOffset = token.Offset + token.Text.Length;
                            batchLineNumber = token.Line;
                            batchColumnNumber = token.Column + token.Text.Length;
                        }
                        else
                        {
                            // Append to current batch
                            batchSql.Append(token.Text);
                        }
                    }

                    // Process the last batch if any
                    ProcessAccumulatedBatch();
                }

                // Create parsed script from batches
                var result = ParsedScript.FromBatches(batches, scriptText, source);
                result.Errors = errors;
                result.TokenStream = tokenStream;

                // Track performance
                _performanceTracker.IncrementCounter("Parsing", "TotalScripts");
                _performanceTracker.IncrementCounter("Parsing", "TotalFragments", fragmentsProcessed);

                return result;
            }
            catch (OperationCanceledException)
            {
                _performanceTracker.IncrementCounter("Parsing", "Cancelled");
                throw;
            }
            catch (Exception ex)
            {
                _performanceTracker.IncrementCounter("Parsing", "Errors");
                _performanceTracker.IncrementCounter("Parsing", "ExceptionErrors");

                var error = new ParseError
                {
                    Message = ex.Message,
                    ErrorCode = -1,
                    Line = 0,
                    Column = 0
                };

                errors.Add(error);
                return CreateErrorResult(scriptText, source, errors, options);
            }
            finally
            {
                // Return parser to pool
                if (parser != null)
                {
                    ParserFactory.ReturnParser(parser);
                }
            }
        }

        /// <summary>
        /// Processes a parsed script AST to extract fragments
        /// </summary>
        private ParsedScript ProcessParsedScript(TSqlFragment scriptAst, IList<TSqlParserToken> tokenStream, string scriptText, string source, 
            List<ParseError> errors, ParsingOptions options)
        {
            try
            {
                // Extract batches
                var batches = new List<SqlFragment>();
                
                // Process based on the root type
                if (scriptAst is TSqlScript script)
                {
                    // Extract batches from script
                    foreach (var batch in script.Batches)
                    {
                        var batchFragment = ProcessBatch(batch, scriptText, batch.StartOffset, 
                            batch.StartLine, batch.StartColumn, options);
                        batches.Add(batchFragment);
                    }
                }
                else if (scriptAst is TSqlBatch batch)
                {
                    // Single batch 
                    var batchFragment = ProcessBatch(batch, scriptText, batch.StartOffset, 
                        batch.StartLine, batch.StartColumn, options);
                    batches.Add(batchFragment);
                }
                else
                {
                    // Try to wrap in a batch
                    var fragment = ProcessFragment(scriptAst, scriptText, scriptAst.StartOffset, 
                        scriptAst.StartLine, scriptAst.StartColumn, options);
                    
                    // Create a synthetic batch
                    var syntheticBatch = new SqlFragment
                    {
                        FragmentType = SqlFragmentType.Batch,
                        SqlText = scriptText,
                        StartOffset = 0,
                        EndOffset = scriptText.Length,
                        LineNumber = 1,
                        ColumnNumber = 1,
                        Statement = "BATCH",
                        ParsedFragment = null,
                        Children = new List<SqlFragment> { fragment },
                        TableReferences = new List<TableReference>(),
                        ColumnReferences = new List<ColumnReference>()
                    };
                    
                    batches.Add(syntheticBatch);
                    fragment.ParentBatch = syntheticBatch;
                }

                // Create parsed script from batches
                var result = ParsedScript.FromBatches(batches, scriptText, source);
                result.Errors = errors;
                result.ScriptAst = scriptAst;
                result.TokenStream = tokenStream;

                // Track performance
                _performanceTracker.IncrementCounter("Parsing", "TotalScripts");
                _performanceTracker.IncrementCounter("Parsing", "TotalFragments", result.TotalFragmentCount);

                return result;
            }
            catch (Exception ex)
            {
                _performanceTracker.IncrementCounter("Parsing", "ProcessingErrors");

                var error = new ParseError
                {
                    Message = $"Error processing parsed script: {ex.Message}",
                    ErrorCode = -1,
                    Line = 0,
                    Column = 0
                };
                
                errors.Add(error);
                return CreateErrorResult(scriptText, source, errors, options);
            }
        }

        /// <summary>
        /// Processes a parsed batch to extract fragments
        /// </summary>
        private SqlFragment ProcessBatch(TSqlFragment batchAst, string scriptText, int startOffset, int lineNumber, int columnNumber, ParsingOptions options)
        {
            // Extract batch text
            string batchText = ExtractFragmentText(scriptText, batchAst.StartOffset, batchAst.EndOffset);

            // Create batch fragment
            var batchFragment = new SqlFragment
            {
                FragmentType = SqlFragmentType.Batch,
                SqlText = batchText,
                StartOffset = batchAst.StartOffset,
                EndOffset = batchAst.EndOffset,
                LineNumber = batchAst.StartLine,
                ColumnNumber = batchAst.StartColumn,
                Statement = "BATCH",
                ParsedFragment = batchAst,
                Children = new List<SqlFragment>(),
                TableReferences = new List<TableReference>()
            };

            // Process statements in batch
            if (batchAst is TSqlBatch batch)
            {
                foreach (var statement in batch.Statements)
                {
                    var fragment = ProcessFragment(statement, scriptText, statement.StartOffset, 
                        statement.StartLine, statement.StartColumn, options);
                    
                    batchFragment.Children.Add(fragment);
                    fragment.ParentBatch = batchFragment;
                    
                    // Collect table references from children
                    batchFragment.TableReferences.AddRange(fragment.TableReferences);
                }
            }

            return batchFragment;
        }

        /// <summary>
        /// Processes a parsed fragment to extract metadata
        /// </summary>
        private SqlFragment ProcessFragment(TSqlFragment fragmentAst, string scriptText, int startOffset, int lineNumber, int columnNumber, ParsingOptions options)
        {
            // Determine fragment type
            SqlFragmentType fragmentType = ReferenceExtractor.DetermineFragmentType(fragmentAst);

            // Extract statement text
            string fragmentText = ExtractFragmentText(scriptText, fragmentAst.StartOffset, fragmentAst.EndOffset);
            
            // Determine statement type name
            string statementType = fragmentAst.GetType().Name;
            
            // Create fragment
            var fragment = new SqlFragment
            {
                FragmentType = fragmentType,
                SqlText = fragmentText,
                StartOffset = fragmentAst.StartOffset,
                EndOffset = fragmentAst.EndOffset,
                LineNumber = fragmentAst.StartLine,
                ColumnNumber = fragmentAst.StartColumn,
                Statement = statementType,
                ParsedFragment = fragmentAst,
                Children = new List<SqlFragment>(),
                TableReferences = new List<TableReference>(),
                ColumnReferences = new List<ColumnReference>()
            };

            // Extract table references if enabled
            if (options.ExtractTableReferences)
            {
                fragment.TableReferences = ReferenceExtractor.ExtractTableReferences(fragmentAst, options);
            }

            // Extract column references if enabled
            if (options.ExtractColumnReferences)
            {
                fragment.ColumnReferences = ReferenceExtractor.ExtractColumnReferences(fragmentAst, options, fragment.TableReferences);
            }

            // Process child elements (for specific statement types)
            if (fragmentAst is SelectStatement selectStmt)
            {
                ProcessSelectStatementChildren(fragment, selectStmt, scriptText, options);
            }
            else if (fragmentAst is InsertStatement insertStmt)
            {
                ProcessInsertStatementChildren(fragment, insertStmt, scriptText, options);
            }
            else if (fragmentAst is UpdateStatement updateStmt)
            {
                ProcessUpdateStatementChildren(fragment, updateStmt, scriptText, options);
            }
            else if (fragmentAst is MergeStatement mergeStmt)
            {
                ProcessMergeStatementChildren(fragment, mergeStmt, scriptText, options);
            }
            else if (fragmentAst is IfStatement ifStmt)
            {
                ProcessIfStatementChildren(fragment, ifStmt, scriptText, options);
            }
            else if (fragmentAst is WhileStatement whileStmt)
            {
                ProcessWhileStatementChildren(fragment, whileStmt, scriptText, options);
            }
            else if (fragmentAst is BeginEndBlockStatement beginEndStmt)
            {
                ProcessBeginEndBlockChildren(fragment, beginEndStmt, scriptText, options);
            }

            return fragment;
        }

        #region Statement-specific processing

        /// <summary>
        /// Processes children of a SELECT statement
        /// </summary>
        private void ProcessSelectStatementChildren(SqlFragment parentFragment, SelectStatement selectStmt, string scriptText, ParsingOptions options)
        {
            // Process CTE if present
            if (selectStmt.WithCtesAndXmlNamespaces != null && 
                selectStmt.WithCtesAndXmlNamespaces.CommonTableExpressions.Count > 0)
            {
                foreach (var cte in selectStmt.WithCtesAndXmlNamespaces.CommonTableExpressions)
                {
                    var cteFragment = ProcessFragment(cte, scriptText, cte.StartOffset, cte.StartLine, cte.StartColumn, options);
                    parentFragment.Children.Add(cteFragment);
                }
            }

            // Process query expression (subqueries, joins, etc.)
            if (selectStmt.QueryExpression != null)
            {
                if (selectStmt.QueryExpression is QuerySpecification querySpec)
                {
                    ProcessQuerySpecificationChildren(parentFragment, querySpec, scriptText, options);
                }
                else if (selectStmt.QueryExpression is BinaryQueryExpression binaryQuery)
                {
                    ProcessBinaryQueryChildren(parentFragment, binaryQuery, scriptText, options);
                }
            }
        }

        /// <summary>
        /// Processes children of a QuerySpecification
        /// </summary>
        private void ProcessQuerySpecificationChildren(SqlFragment parentFragment, QuerySpecification querySpec, string scriptText, ParsingOptions options)
        {
            // Process FROM clause (tables, joins)
            if (querySpec.FromClause != null)
            {
                foreach (var tableRef in querySpec.FromClause.TableReferences)
                {
                    if (tableRef is QualifiedJoin qualifiedJoin)
                    {
                        var joinFragment = new SqlFragment
                        {
                            FragmentType = SqlFragmentType.Join,
                            SqlText = ExtractFragmentText(scriptText, qualifiedJoin.StartOffset, qualifiedJoin.EndOffset),
                            StartOffset = qualifiedJoin.StartOffset,
                            EndOffset = qualifiedJoin.EndOffset,
                            LineNumber = qualifiedJoin.StartLine,
                            ColumnNumber = qualifiedJoin.StartColumn,
                            Statement = qualifiedJoin.GetType().Name,
                            ParsedFragment = qualifiedJoin,
                            Children = new List<SqlFragment>(),
                            TableReferences = ReferenceExtractor.ExtractTableReferences(qualifiedJoin, options),
                            ColumnReferences = ReferenceExtractor.ExtractColumnReferences(qualifiedJoin, options)
                        };
                        
                        parentFragment.Children.Add(joinFragment);
                    }
                    else if (tableRef is TableReferenceWithAlias tblWithAlias && 
                             tblWithAlias is DerivedTable derivedTable && 
                             derivedTable.QueryExpression != null)
                    {
                        // Process derived tables/subqueries
                        var subqueryFragment = new SqlFragment
                        {
                            FragmentType = SqlFragmentType.Subquery,
                            SqlText = ExtractFragmentText(scriptText, derivedTable.StartOffset, derivedTable.EndOffset),
                            StartOffset = derivedTable.StartOffset,
                            EndOffset = derivedTable.EndOffset,
                            LineNumber = derivedTable.StartLine,
                            ColumnNumber = derivedTable.StartColumn,
                            Statement = "Subquery",
                            ParsedFragment = derivedTable,
                            Children = [],
                            TableReferences = ReferenceExtractor.ExtractTableReferences(derivedTable, options),
                            ColumnReferences = ReferenceExtractor.ExtractColumnReferences(derivedTable, options)
                        };
                        
                        parentFragment.Children.Add(subqueryFragment);
                    }
                }
            }

            // Process WHERE clause for subqueries
            if (querySpec.WhereClause != null && querySpec.WhereClause.SearchCondition != null)
            {
                // Look for subquery expressions in the WHERE clause
                var subqueryVisitor = new SubqueryVisitor();
                querySpec.WhereClause.Accept(subqueryVisitor);
                
                foreach (var subquery in subqueryVisitor.Subqueries)
                {
                    var subqueryFragment = new SqlFragment
                    {
                        FragmentType = SqlFragmentType.Subquery,
                        SqlText = ExtractFragmentText(scriptText, subquery.StartOffset, subquery.EndOffset),
                        StartOffset = subquery.StartOffset,
                        EndOffset = subquery.EndOffset,
                        LineNumber = subquery.StartLine,
                        ColumnNumber = subquery.StartColumn,
                        Statement = "Subquery",
                        ParsedFragment = subquery,
                        Children = [],
                        TableReferences = ReferenceExtractor.ExtractTableReferences(subquery, options),
                        ColumnReferences = ReferenceExtractor.ExtractColumnReferences(subquery, options)
                    };
                    
                    parentFragment.Children.Add(subqueryFragment);
                }
            }
        }

        /// <summary>
        /// Processes children of a binary query (UNION, INTERSECT, EXCEPT)
        /// </summary>
        private void ProcessBinaryQueryChildren(SqlFragment parentFragment, BinaryQueryExpression binaryQuery, string scriptText, ParsingOptions options)
        {
            // Process first query
            if (binaryQuery.FirstQueryExpression != null)
            {
                if (binaryQuery.FirstQueryExpression is QuerySpecification querySpec)
                {
                    ProcessQuerySpecificationChildren(parentFragment, querySpec, scriptText, options);
                }
                else if (binaryQuery.FirstQueryExpression is BinaryQueryExpression nestedBinary)
                {
                    ProcessBinaryQueryChildren(parentFragment, nestedBinary, scriptText, options);
                }
            }

            // Process second query
            if (binaryQuery.SecondQueryExpression != null)
            {
                if (binaryQuery.SecondQueryExpression is QuerySpecification querySpec)
                {
                    ProcessQuerySpecificationChildren(parentFragment, querySpec, scriptText, options);
                }
                else if (binaryQuery.SecondQueryExpression is BinaryQueryExpression nestedBinary)
                {
                    ProcessBinaryQueryChildren(parentFragment, nestedBinary, scriptText, options);
                }
            }
        }

        /// <summary>
        /// Processes children of an INSERT statement
        /// </summary>
        private void ProcessInsertStatementChildren(SqlFragment parentFragment, InsertStatement insertStmt, string scriptText, ParsingOptions options)
        {
            if (insertStmt.InsertSpecification?.InsertSource is SelectInsertSource selectSource && 
                selectSource.Select != null)
            {
                var selectFragment = ProcessFragment(selectSource.Select, scriptText, 
                    selectSource.Select.StartOffset, selectSource.Select.StartLine, 
                    selectSource.Select.StartColumn, options);
                
                parentFragment.Children.Add(selectFragment);
            }
        }

        /// <summary>
        /// Processes children of an UPDATE statement
        /// </summary>
        private void ProcessUpdateStatementChildren(SqlFragment parentFragment, UpdateStatement updateStmt, string scriptText, ParsingOptions options)
        {
            // Process subqueries in SET clauses
            if (updateStmt.UpdateSpecification?.SetClauses != null)
            {
                foreach (var setClause in updateStmt.UpdateSpecification.SetClauses)
                {
                    if (setClause is AssignmentSetClause assignmentClause && 
                        assignmentClause.NewValue is ScalarSubquery subquery)
                    {
                        var subqueryFragment = ProcessFragment(subquery, scriptText, 
                            subquery.StartOffset, subquery.StartLine, subquery.StartColumn, options);
                        
                        parentFragment.Children.Add(subqueryFragment);
                    }
                }
            }

            // Process WHERE clause subqueries
            if (updateStmt.UpdateSpecification?.WhereClause?.SearchCondition != null)
            {
                var subqueryVisitor = new SubqueryVisitor();
                updateStmt.UpdateSpecification.WhereClause.Accept(subqueryVisitor);
                
                foreach (var subquery in subqueryVisitor.Subqueries)
                {
                    var subqueryFragment = ProcessFragment(subquery, scriptText, 
                        subquery.StartOffset, subquery.StartLine, subquery.StartColumn, options);
                    
                    parentFragment.Children.Add(subqueryFragment);
                }
            }
        }

        /// <summary>
        /// Processes children of a MERGE statement
        /// </summary>
        private void ProcessMergeStatementChildren(SqlFragment parentFragment, MergeStatement mergeStmt, string scriptText, ParsingOptions options)
        {
            // Process each action clause
            if (mergeStmt.ActionClauses != null)
            {
                foreach (var actionClause in mergeStmt.ActionClauses)
                {
                    if (actionClause is MergeInsertClause insertClause && insertClause.Source is SelectInsertSource selectSource)
                    {
                        var selectFragment = ProcessFragment(selectSource.Select, scriptText, 
                            selectSource.Select.StartOffset, selectSource.Select.StartLine, 
                            selectSource.Select.StartColumn, options);
                        
                        parentFragment.Children.Add(selectFragment);
                    }
                }
            }
        }

        /// <summary>
        /// Processes children of an IF statement
        /// </summary>
        private void ProcessIfStatementChildren(SqlFragment parentFragment, IfStatement ifStmt, string scriptText, ParsingOptions options)
        {
            // Process the IF body
            if (ifStmt.ThenStatement != null)
            {
                var thenFragment = ProcessFragment(ifStmt.ThenStatement, scriptText, 
                    ifStmt.ThenStatement.StartOffset, ifStmt.ThenStatement.StartLine, 
                    ifStmt.ThenStatement.StartColumn, options);
                
                parentFragment.Children.Add(thenFragment);
            }

            // Process the ELSE body
            if (ifStmt.ElseStatement != null)
            {
                var elseFragment = ProcessFragment(ifStmt.ElseStatement, scriptText, 
                    ifStmt.ElseStatement.StartOffset, ifStmt.ElseStatement.StartLine, 
                    ifStmt.ElseStatement.StartColumn, options);
                
                parentFragment.Children.Add(elseFragment);
            }
        }

        /// <summary>
        /// Processes children of a WHILE statement
        /// </summary>
        private void ProcessWhileStatementChildren(SqlFragment parentFragment, WhileStatement whileStmt, string scriptText, ParsingOptions options)
        {
            // Process the statement body
            if (whileStmt.Statement != null)
            {
                var bodyFragment = ProcessFragment(whileStmt.Statement, scriptText, 
                    whileStmt.Statement.StartOffset, whileStmt.Statement.StartLine, 
                    whileStmt.Statement.StartColumn, options);
                
                parentFragment.Children.Add(bodyFragment);
            }
        }

        /// <summary>
        /// Processes children of a BEGIN-END block
        /// </summary>
        private void ProcessBeginEndBlockChildren(SqlFragment parentFragment, BeginEndBlockStatement blockStmt, string scriptText, ParsingOptions options)
        {
            // Process each statement in the block
            if (blockStmt.StatementList?.Statements != null)
            {
                foreach (var stmt in blockStmt.StatementList.Statements)
                {
                    var stmtFragment = ProcessFragment(stmt, scriptText, 
                        stmt.StartOffset, stmt.StartLine, stmt.StartColumn, options);
                    
                    parentFragment.Children.Add(stmtFragment);
                }
            }
        }

        #endregion

        /// <summary>
        /// Extracts the text of a fragment from the original script
        /// </summary>
        private static string ExtractFragmentText(string scriptText, int startOffset, int endOffset)
        {
            if (string.IsNullOrEmpty(scriptText) || startOffset < 0 || endOffset <= startOffset || endOffset > scriptText.Length)
                return string.Empty;

            return scriptText[startOffset..endOffset];
        }

        /// <summary>
        /// Creates a parsed script with errors
        /// </summary>
        private static ParsedScript CreateErrorResult(string scriptText, string source, List<ParseError> errors, ParsingOptions options)
        {
            return new ParsedScript
            {
                ScriptText = scriptText,
                Source = source,
                Errors = errors,
                Fragments = [],
                Batches = [],
                TableReferences = new Dictionary<string, List<TableReference>>(StringComparer.OrdinalIgnoreCase),
                ColumnReferences = new Dictionary<string, List<ColumnReference>>(StringComparer.OrdinalIgnoreCase)
            };
        }

        /// <summary>
        /// Visitor to find subqueries in a TSqlFragment
        /// </summary>
        private class SubqueryVisitor : TSqlFragmentVisitor
        {
            public List<ScalarSubquery> Subqueries { get; } = new List<ScalarSubquery>();

            public override void ExplicitVisit(ScalarSubquery node)
            {
                Subqueries.Add(node);
                base.ExplicitVisit(node);
            }
        }
    }
}