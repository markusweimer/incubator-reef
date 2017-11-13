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

using System.IO;
using Org.Apache.REEF.Client.API.Testing;
using Org.Apache.REEF.Client.Local.TestRunner.FileWritingAssert;
using Org.Apache.REEF.Client.YARN.TestRunner.IFileSystemAssert;
using Org.Apache.REEF.IO.FileSystem.Local;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Xunit;

namespace Org.Apache.REEF.Client.Tests
{
    /// <summary>
    /// Tests for FileSystemAssert
    /// </summary>
    public sealed class FileSystemAssertTest
    {
        [Fact]
        public void TestFileSystemAssert()
        {
            var assertPath = Path.GetTempFileName();
            var x = MakeInstance(assertPath);
            File.Delete(assertPath);

            x.True(true, "Something went right.");
            var testResult1 = Read(assertPath);
            Assert.Equal(1, testResult1.NumberOfPassedAsserts);
            Assert.Equal(0, testResult1.NumberOfFailedAsserts);

            x.Fail("Something went wrong.");
            x.True(false, "Something else went wrong");
            x.False(false, "Something else went right.");
            var testResult2 = Read(assertPath);
            Assert.Equal(2, testResult2.NumberOfPassedAsserts);
            Assert.Equal(1, testResult2.NumberOfFailedAsserts);
        }

        /// <summary>
        /// Reads a TestResult from the given file.
        /// </summary>
        /// <param name="assertPath">The file to read from.</param>
        /// <returns>The TestResult read from the file.</returns>
        private static ITestResult Read(string assertPath)
        {
            return TestResult.FromJson(File.ReadAllText(assertPath));
        }

        /// <summary>
        /// Creates an instance of FileSystemAssert using the local file system.
        /// </summary>
        /// <param name="assertPath">The file name of the assert file.</param>
        /// <returns>An instance of FileSystemAssert using the local file system.</returns>
        private static FileSystemAssert MakeInstance(string assertPath)
        {
            var tang = TangFactory.GetTang();

            var fileSystemConfiguration = LocalFileSystemConfiguration.ConfigurationModule.Build();

            var assertConfiguration = tang.NewConfigurationBuilder()
                .BindImplementation<IAssert, FileSystemAssert>()
                .BindNamedParameter(typeof(YARN.TestRunner.IFileSystemAssert.Parameters.AssertFilePath), assertPath)
                .Build();

            var finalConfiguration = Configurations.Merge(fileSystemConfiguration, assertConfiguration);

            return tang.NewInjector(finalConfiguration).GetInstance<FileSystemAssert>();
        }
    }
}