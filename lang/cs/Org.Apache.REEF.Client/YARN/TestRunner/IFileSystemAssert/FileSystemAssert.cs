// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
// 
//   http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using System;
using Org.Apache.REEF.Client.API.Testing;
using Org.Apache.REEF.Client.Local.TestRunner.FileWritingAssert;
using Org.Apache.REEF.Client.YARN.TestRunner.IFileSystemAssert.Parameters;
using Org.Apache.REEF.IO.FileSystem;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Client.YARN.TestRunner.IFileSystemAssert
{
    /// <inheritdoc />
    /// <summary>
    /// An implementation of IAssert using a file stored on an `IFileSystem`.
    /// </summary>
    [Unstable("0.17", "Work in progress towards a new test infrastructure. See REEF-1271.")]
    internal sealed class FileSystemAssert : AbstractAssert
    {
        private static readonly Logger LOG = Logger.GetLogger(typeof(FileSystemAssert));

        /// <summary>
        /// Keeps the results of the test thus far.
        /// </summary>
        private readonly TestResult _testResult = new TestResult();

        /// <summary>
        /// The `IFileSystem` instance used.
        /// </summary>
        private readonly IFileSystem _fileSystem;

        /// <summary>
        /// The URI under which the assert file will be written.
        /// </summary>
        private readonly Uri _assertUri;

        /// <summary>
        /// The name of the local temp file used to stage the asserts.
        /// </summary>
        private readonly string _localAssertFileName;

        /// <summary>
        /// The lock used when writing to the DFS.
        /// </summary>
        private readonly object _writeLock = new object();

        /// <summary>
        /// Records whether we have written the file to the DFS at least once.
        /// </summary>
        private bool _fileWrittenOnce = false;

        /// <summary>
        /// Instantiates a new FileSystemAssert.
        /// </summary>
        /// <param name="fileSystem">The `IFileSystem` instance to be used.</param>
        /// <param name="assertPathOnDfs">The path on the `IFileSystem` where the assert file is to be written.</param>
        /// <param name="localAssertFileName">The name of the local file to be used to stage the asserts.</param>
        [Inject]
        private FileSystemAssert(IFileSystem fileSystem,
            [Parameter(typeof(Parameters.AssertFilePath))] string assertPathOnDfs,
            [Parameter(typeof(Parameters.LocalAssertFileName))] string localAssertFileName)
        {
            // Parameter checks.
            if (fileSystem == null)
            {
                throw new ArgumentNullException(nameof(fileSystem));
            }
            if (string.IsNullOrWhiteSpace(assertPathOnDfs))
            {
                throw new ArgumentNullException(nameof(assertPathOnDfs));
            }
            if (string.IsNullOrWhiteSpace(localAssertFileName))
            {
                throw new ArgumentNullException(nameof(LocalAssertFileName));
            }

            // Set attributes.
            _fileSystem = fileSystem;
            _localAssertFileName = localAssertFileName;
            _assertUri = _fileSystem.CreateUriForPath(assertPathOnDfs);
        }

        public override void True(bool condition, string format, params object[] args)
        {
            // Parameter checks.
            if (string.IsNullOrWhiteSpace(format))
            {
                throw new ArgumentNullException(nameof(format), "Asserts must have a message.");
            }

            // Writes are locked, as there might be multiple in flight and can't append to the file yet.
            // TODO[JIRA REEF-1960]: Instead, we should keep `_testResult` in memory and write it during `DriverStop`.
            lock (_writeLock)
            {
                // Record the test result.
                LOG.Log(Level.Verbose, "Recording assert");
                _testResult.Add(condition, format, args);

                // Write those to a local file.
                LOG.Log(Level.Verbose, "Writing asserts to local file `{0}`.", _localAssertFileName);
                System.IO.File.WriteAllText(_localAssertFileName, _testResult.ToJson());

                // Delete the assert file on the distributed filesystem, but only if we've written it before.
                if (_fileWrittenOnce)
                {
                    LOG.Log(Level.Verbose, "Deleting assert file on DFS: `{0}`.", _assertUri);
                    _fileSystem.Delete(_assertUri);
                }

                // Upload the new copy.
                LOG.Log(Level.Verbose, "Uploading local assert file `{0}` to DFS URI `{1}`.",
                    _localAssertFileName, _assertUri);
                _fileSystem.CopyFromLocal(_localAssertFileName, _assertUri);
                _fileWrittenOnce = true;
            }
        }
    }
}