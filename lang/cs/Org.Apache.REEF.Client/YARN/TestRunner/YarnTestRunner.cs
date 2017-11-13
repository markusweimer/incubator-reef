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
using System.IO;
using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Client.API.Testing;
using Org.Apache.REEF.Client.Common;
using Org.Apache.REEF.Client.Local.TestRunner.FileWritingAssert;
using Org.Apache.REEF.Client.Yarn;
using Org.Apache.REEF.Client.YARN.TestRunner.IFileSystemAssert;
using Org.Apache.REEF.IO.FileSystem;
using Org.Apache.REEF.IO.FileSystem.Hadoop;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Client.YARN.TestRunner
{
    /// <inheritdoc />
    /// <summary>
    /// YARN implementation of ITestRunner.
    /// </summary>
    [Unstable("0.17", "Work in progress towards a new test infrastructure. See REEF-1271.")]
    internal sealed class YarnTestRunner : ITestRunner
    {
        private static readonly Logger LOG = Logger.GetLogger(typeof(YarnTestRunner));

        /// <summary>
        /// The REEF Client used to submit the job.
        /// </summary>
        private readonly IREEFClient _client;

        /// <summary>
        /// The `IFileSystem` instance used to store & retrieve the assert files.
        /// </summary>
        private readonly IFileSystem _fileSystem;

        /// <summary>
        /// A prefix applied to the file name of the assert file on the DFS.
        /// </summary>
        private readonly string _assertPathPrefix;

        /// <summary>
        /// Instantiates a new `YarnTestRunner`.
        /// </summary>
        /// <param name="client">The REEF Client used to submit the job.</param>
        /// <param name="fileSystem">The `IFileSystem` instance used to store & retrieve the assert files.</param>
        /// <param name="assertPathPrefix">A prefix applied to the file name of the assert file on the DFS.</param>
        [Inject]
        private YarnTestRunner(IREEFClient client, IFileSystem fileSystem,
            [Parameter(typeof(Parameters.AssertFilePathPrefix))] string assertPathPrefix)
        {
            _client = client;
            _fileSystem = fileSystem;
            _assertPathPrefix = assertPathPrefix;
        }

        public JobRequestBuilder NewJobRequestBuilder()
        {
            return _client.NewJobRequestBuilder();
        }

        public ITestResult RunTest(JobRequestBuilder jobRequestBuilder)
        {
            // Assemble the job submission.
            string assertPath = MakeAssertPath();
            var jobRequest = jobRequestBuilder.AddDriverConfiguration(MakeDriverConfiguration(assertPath)).Build();

            // Submit the job.
            LOG.Log(Level.Info, "Submitting job `{0}` for execution. Assert log in `{1}`",
                jobRequest.JobIdentifier,
                assertPath);
            IJobSubmissionResult jobStatus = _client.SubmitAndGetJobStatus(jobRequest);

            if (jobStatus == null)
            {
                return TestResult.Fail(
                    "JobStatus returned by the Client was null. This points to an environment setup problem.");
            }

            // Wait for the Driver to complete.
            LOG.Log(Level.Verbose, "Waiting for job `{0}` to complete.", jobRequest.JobIdentifier);
            jobStatus.WaitForDriverToFinish();
            LOG.Log(Level.Verbose, "Job `{0}` completed.", jobRequest.JobIdentifier);

            // Read the results from the DFS and return them.
            return ReadTestResult(assertPath);
        }

        /// <summary>
        /// Assemble the path under which the assert file should be stored on the DFS.
        /// </summary>
        /// <returns>the path under which the assert file should be stored on the DFS.</returns>
        private string MakeAssertPath()
        {
            return _assertPathPrefix + "reef-assert-" + DateTime.Now.ToString("yyyyMMddHHmmssfff") + ".json";
        }

        /// <summary>
        /// Reads the test results from the DFS.
        /// </summary>
        /// <param name="assertPath">The path to the assert file.</param>
        /// <returns>The test results read from the DFS.</returns>
        private ITestResult ReadTestResult(string assertPath)
        {
            // Validate parameters
            if (string.IsNullOrWhiteSpace(assertPath))
            {
                throw new ArgumentNullException(nameof(assertPath));
            }

            // Parse the URI
            Uri assertUri = _fileSystem.CreateUriForPath(assertPath);

            // Check that the file exists.
            if (!_fileSystem.Exists(assertUri))
            {
                return TestResult.Fail("Test Results file {0} does not exist.", assertUri);
            }

            // Copy the file to the local file system
            string localFileName = Path.GetTempFileName();
            try
            {
                _fileSystem.CopyToLocal(assertUri, localFileName);
            }
            catch (Exception exception)
            {
                return TestResult.Fail("Could not copy test results from `{0}` to `{1}` because of `{2}`",
                    assertUri, localFileName, exception);
            }

            // Parse the file and return the results.
            try
            {
                return TestResult.FromJson(File.ReadAllText(localFileName))
                       ?? TestResult.Fail("Results read from `{0}` were null.", assertUri);
            }
            catch (Exception exception)
            {
                return TestResult.Fail("Could not parse test results in file `{0}` because of `{1}`", assertUri, exception);
            }
        }

        /// <summary>
        /// Generates the Driver Configuration for the FileSystemAssert.
        /// </summary>
        /// <param name="assertPath">Where to store the assert file on the DFS.</param>
        /// <returns>The Driver Configuration for the FileSystemAssert.</returns>
        private static IConfiguration MakeDriverConfiguration(string assertPath)
        {
            var assertConfiguration = TangFactory.GetTang().NewConfigurationBuilder()
                .BindImplementation<IAssert, FileSystemAssert>()
                .BindNamedParameter(typeof(IFileSystemAssert.Parameters.AssertFilePath), assertPath)
                .Build();

            return assertConfiguration;
        }

        /// <summary>
        /// Instantiate a new `ITestRunner` for the YARN runtime.
        /// </summary>
        /// <param name="runtimeConfiguration">The configuration of the YARN runtime. Defaults to the default YARN configuration if null.</param>
        /// <param name="fileSystemConfiguration">The filesystem configuration to be used. Defaults to `HadoopFileSystemConfiguration` if null.</param>
        /// <param name="assertFilePrefix">The prefix use for the assert file on the filesystem. Defaults to `/tmp/`</param>
        /// <returns>A new `ITestRunner` for the YARN runtime.</returns>
        internal static ITestRunner GetYarnTestRunner(IConfiguration runtimeConfiguration = null,
            IConfiguration fileSystemConfiguration = null,
            string assertFilePrefix = "/tmp/")
        {
            if (runtimeConfiguration == null)
            {
                runtimeConfiguration = YARNClientConfiguration.ConfigurationModule.Build();
            }

            if (fileSystemConfiguration == null)
            {
                fileSystemConfiguration = HadoopFileSystemConfiguration.ConfigurationModule.Build();
            }

            var tang = TangFactory.GetTang();

            IConfiguration assertConfiguration = tang.NewConfigurationBuilder()
                .BindNamedParameter(typeof(Parameters.AssertFilePathPrefix), assertFilePrefix)
                .Build();

            var configuration = Configurations.Merge(runtimeConfiguration,
                fileSystemConfiguration,
                assertConfiguration);

            return tang.NewInjector(configuration).GetInstance<YarnTestRunner>();
        }
    }
}