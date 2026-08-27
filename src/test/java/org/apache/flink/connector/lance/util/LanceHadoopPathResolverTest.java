/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.lance.util;

import org.apache.hadoop.conf.Configuration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link LanceHadoopPathResolver}.
 *
 * <p>Focus areas:
 * <ul>
 *   <li>Native Lance schemes should be passed through unchanged.</li>
 *   <li>Local file URIs should be passed through unchanged.</li>
 *   <li>Hadoop-family schemes should trigger download to local cache and return {@code file://} path.</li>
 *   <li>Scheme detection utility should be case-insensitive and null-safe.</li>
 * </ul>
 */
class LanceHadoopPathResolverTest {

    @TempDir
    Path tempDir;

    @Test
    void extractSchemeReturnsLowercaseScheme() {
        assertThat(LanceHadoopPathResolver.extractScheme("S3://bucket/foo")).isEqualTo("s3");
        assertThat(LanceHadoopPathResolver.extractScheme("TBDSFS://svc/foo")).isEqualTo("tbdsfs");
        assertThat(LanceHadoopPathResolver.extractScheme("hdfs://nn/p")).isEqualTo("hdfs");
        assertThat(LanceHadoopPathResolver.extractScheme("file:///tmp/x")).isEqualTo("file");
    }

    @Test
    void extractSchemeReturnsNullForBarePath() {
        assertThat(LanceHadoopPathResolver.extractScheme("/tmp/foo/bar")).isNull();
        assertThat(LanceHadoopPathResolver.extractScheme("relative/path")).isNull();
    }

    @Test
    void normalizeUriRestoresMissingSlashInAuthority() {
        // 场景 1：Flink Path 反序列化把 "tbdsfs://internal/x" 变成 "tbdsfs:/internal/x"，
        // normalizeUri 应恢复
        assertThat(LanceHadoopPathResolver.normalizeUri("tbdsfs:/internal/lance_poc/db1"))
                .isEqualTo("tbdsfs://internal/lance_poc/db1");
        // 场景 2：hdfs://ns1/path 也一样
        assertThat(LanceHadoopPathResolver.normalizeUri("hdfs:/ns1/path"))
                .isEqualTo("hdfs://ns1/path");
    }

    @Test
    void normalizeUriIsIdempotent() {
        // 已经是双斜杠不变
        assertThat(LanceHadoopPathResolver.normalizeUri("tbdsfs://internal/x"))
                .isEqualTo("tbdsfs://internal/x");
        // 空 authority 的 "scheme:///" 也不变
        assertThat(LanceHadoopPathResolver.normalizeUri("file:///tmp/x"))
                .isEqualTo("file:///tmp/x");
        // 本地路径不变
        assertThat(LanceHadoopPathResolver.normalizeUri("/tmp/x")).isEqualTo("/tmp/x");
        assertThat(LanceHadoopPathResolver.normalizeUri(null)).isNull();
    }

    @Test
    void isNativeSupportedSchemeRecognizesLanceSchemes() {
        assertThat(LanceHadoopPathResolver.isNativeSupportedScheme("s3")).isTrue();
        assertThat(LanceHadoopPathResolver.isNativeSupportedScheme("S3")).isTrue();
        assertThat(LanceHadoopPathResolver.isNativeSupportedScheme("file")).isTrue();
        assertThat(LanceHadoopPathResolver.isNativeSupportedScheme("cos")).isTrue();
        assertThat(LanceHadoopPathResolver.isNativeSupportedScheme("tbdsfs")).isFalse();
        assertThat(LanceHadoopPathResolver.isNativeSupportedScheme("hdfs")).isFalse();
        assertThat(LanceHadoopPathResolver.isNativeSupportedScheme(null)).isFalse();
    }

    @Test
    void resolveForReadPassesThroughNullAndEmpty() {
        assertThat(LanceHadoopPathResolver.resolveForRead(null, null, null)).isNull();
        assertThat(LanceHadoopPathResolver.resolveForRead("", null, null)).isEmpty();
    }

    @Test
    void resolveForReadPassesThroughLocalPath() {
        String local = "/tmp/lance/dataset.lance";
        String resolved = LanceHadoopPathResolver.resolveForRead(local, null, null);
        assertThat(resolved).isEqualTo(local);
    }

    @Test
    void resolveForReadPassesThroughFileUri() {
        String fileUri = "file:///tmp/lance/dataset.lance";
        String resolved = LanceHadoopPathResolver.resolveForRead(fileUri, null, null);
        assertThat(resolved).isEqualTo(fileUri);
    }

    @Test
    void resolveForReadPassesThroughS3Uri() {
        String s3 = "s3://bucket/prefix/dataset.lance";
        String resolved = LanceHadoopPathResolver.resolveForRead(s3, null, null);
        assertThat(resolved).isEqualTo(s3);
    }

    @Test
    void resolveForReadPassesThroughCosUri() {
        String cos = "cos://bucket/prefix/dataset.lance";
        String resolved = LanceHadoopPathResolver.resolveForRead(cos, null, null);
        assertThat(resolved).isEqualTo(cos);
    }

    /**
     * 使用自定义 scheme {@code mytestfs://}（lance 不原生支持），并通过 Hadoop
     * 配置将该 scheme 重定向到 {@link org.apache.hadoop.fs.LocalFileSystem}，从而完整
     * 走通 Hadoop FS 下载链路，且不依赖真实 HDFS。
     */
    @Test
    void resolveForReadDownloadsFromHadoopFileSystem() throws IOException {
        // 1. 构造一个“源” lance dataset 目录（用本地磁盘模拟）
        Path srcDir = tempDir.resolve("src_dataset.lance");
        Path versionsDir = srcDir.resolve("_versions");
        Path dataDir = srcDir.resolve("data");
        Files.createDirectories(versionsDir);
        Files.createDirectories(dataDir);
        Files.write(versionsDir.resolve("1.manifest"), new byte[] {1, 2, 3, 4});
        Files.write(dataDir.resolve("part-0.lance"), "hello".getBytes());
        Files.write(srcDir.resolve("_latest.manifest"), new byte[] {9});

        // 2. 配置：把自定义 scheme mytestfs 重定向到 Hadoop LocalFileSystem
        Configuration conf = new Configuration(false);
        conf.set("fs.mytestfs.impl", LocalFsWithMytestfsScheme.class.getName());

        // 3. 使用 mytestfs:// 前缀访问源目录，触发 Hadoop FS 下载路径
        String srcUri = "mytestfs://" + srcDir.toAbsolutePath();
        Path cacheRoot = tempDir.resolve("cache");

        String resolved = LanceHadoopPathResolver.resolveForRead(
                srcUri, conf, cacheRoot.toAbsolutePath().toString());

        // 4. 验证返回的是本地 file:// 路径，并且包含预期的文件
        assertThat(resolved).startsWith("file://");
        Path resolvedDir = Path.of(resolved.substring("file://".length()));
        assertThat(Files.isDirectory(resolvedDir)).isTrue();
        assertThat(Files.isDirectory(resolvedDir.resolve("_versions"))).isTrue();
        assertThat(Files.isRegularFile(resolvedDir.resolve("_versions/1.manifest"))).isTrue();
        assertThat(Files.isRegularFile(resolvedDir.resolve("data/part-0.lance"))).isTrue();
        assertThat(Files.readString(resolvedDir.resolve("data/part-0.lance"))).isEqualTo("hello");
    }

    /**
     * 二次 resolve 相同 URI 时应命中缓存，不重复下载。
     */
    @Test
    void resolveForReadReusesCacheOnSecondCall() throws IOException {
        Path srcDir = tempDir.resolve("cached_dataset.lance");
        Path versionsDir = srcDir.resolve("_versions");
        Files.createDirectories(versionsDir);
        Files.write(versionsDir.resolve("1.manifest"), new byte[] {1});

        Configuration conf = new Configuration(false);
        conf.set("fs.mytestfs.impl", LocalFsWithMytestfsScheme.class.getName());

        String srcUri = "mytestfs://" + srcDir.toAbsolutePath();
        Path cacheRoot = tempDir.resolve("cache2");

        String first = LanceHadoopPathResolver.resolveForRead(
                srcUri, conf, cacheRoot.toAbsolutePath().toString());
        // 修改缓存目录里的文件，模拟“已存在的缓存”
        Path firstDir = Path.of(first.substring("file://".length()));
        Path sentinel = firstDir.resolve("_versions/1.manifest");
        Files.write(sentinel, new byte[] {42});

        String second = LanceHadoopPathResolver.resolveForRead(
                srcUri, conf, cacheRoot.toAbsolutePath().toString());
        // 第二次 resolve 应返回同一个目录，且 sentinel 未被重新下载覆盖
        assertThat(second).isEqualTo(first);
        assertThat(Files.readAllBytes(sentinel)).containsExactly(42);
    }

    /**
     * 将 Hadoop {@link org.apache.hadoop.fs.LocalFileSystem} 包装为 {@code mytestfs://} scheme，
     * 测试专用。它仅重写 {@link #getScheme()} 以及将传入的 {@code mytestfs://<abs-path>}
     * URI 重写为能被本地文件系统接受的 {@code file:/<abs-path>} 后转发到 LocalFileSystem。
     */
    public static final class LocalFsWithMytestfsScheme extends org.apache.hadoop.fs.LocalFileSystem {
        @Override
        public String getScheme() {
            return "mytestfs";
        }

        @Override
        public java.net.URI getUri() {
            return java.net.URI.create("mytestfs:///");
        }

        @Override
        public void initialize(java.net.URI name, Configuration conf) throws IOException {
            super.initialize(java.net.URI.create("file:///"), conf);
        }

        @Override
        protected void checkPath(org.apache.hadoop.fs.Path path) {
            // 接受任意 scheme（mytestfs 或 file）
        }

        @Override
        public org.apache.hadoop.fs.Path makeQualified(org.apache.hadoop.fs.Path path) {
            return toLocal(path);
        }

        @Override
        public org.apache.hadoop.fs.FileStatus getFileStatus(org.apache.hadoop.fs.Path f)
                throws IOException {
            return super.getFileStatus(toLocal(f));
        }

        @Override
        public org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus> listFiles(
                org.apache.hadoop.fs.Path f, boolean recursive) throws IOException {
            return super.listFiles(toLocal(f), recursive);
        }

        @Override
        public void copyToLocalFile(boolean delSrc, org.apache.hadoop.fs.Path src,
                                    org.apache.hadoop.fs.Path dst, boolean useRawLocalFileSystem)
                throws IOException {
            super.copyToLocalFile(delSrc, toLocal(src), dst, useRawLocalFileSystem);
        }

        private static org.apache.hadoop.fs.Path toLocal(org.apache.hadoop.fs.Path p) {
            java.net.URI u = p.toUri();
            String path = u.getPath();
            if (u.getAuthority() != null && !u.getAuthority().isEmpty()) {
                // mytestfs://<abs-path> 会把绝对路径的首段解析为 authority
                path = "/" + u.getAuthority() + (path == null ? "" : path);
            }
            return new org.apache.hadoop.fs.Path("file://" + path);
        }
    }
}
