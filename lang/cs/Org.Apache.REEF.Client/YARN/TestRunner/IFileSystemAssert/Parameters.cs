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

using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Client.YARN.TestRunner.IFileSystemAssert.Parameters
{
    /// <inheritdoc />
    /// <summary>
    /// Path under which the Assert file will be stored on the DFS.
    /// </summary>
    [NamedParameter(documentation: "Path under which the Assert file will be stored on the DFS.")]
    [Unstable("0.17", "Work in progress towards a new test infrastructure. See REEF-1271.")]
    internal sealed class AssertFilePath : Name<string>
    {
        private AssertFilePath()
        {
            // Intentionally empty.
        }
    }

    /// <inheritdoc />
    /// <summary>
    /// Name of the local file to use to stage the assert file.
    /// </summary>
    [NamedParameter(documentation: "Name of the local file to use to stage the assert file.", defaultValue: "asserts.json")]
    [Unstable("0.17", "Work in progress towards a new test infrastructure. See REEF-1271.")]
    internal sealed class LocalAssertFileName : Name<string>
    {
        private LocalAssertFileName()
        {
            // Intentionally empty.
        }
    }
}