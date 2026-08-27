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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 方案 B：Hadoop FileSystem 前置缓存层。
 *
 * <p>Lance 底层通过 lance-core（Rust）直接读取存储，其 {@code object_store} 只支持
 * 一组硬编码 scheme（{@code file/s3/gs/az/abfss/oss/cos/hf/memory}）。对于
 * {@code tbdsfs://} / {@code hdfs://} 等 Hadoop 生态的 scheme，Lance 无法识别。
 *
 * <p>本工具类的职责：在 {@code Dataset.open(uri)} 调用之前，如果 {@code uri} 是
 * Hadoop 兼容的 scheme（且不在 Lance 原生支持列表内），则：
 * <ol>
 *   <li>通过 Hadoop {@link FileSystem} 打开源目录；</li>
 *   <li>递归把整个 lance dataset 目录同步到本地临时缓存目录；</li>
 *   <li>返回本地 {@code file:///...} 路径给 Lance。</li>
 * </ol>
 *
 * <p><b>使用条件</b>：需要在 classpath 中提供对应 scheme 的 Hadoop {@code FileSystem}
 * 实现（例如 tbdsfs 需要 {@code tbdsfs-hadoop-*.jar}）。这些 jar 在 TBDS 集群的
 * {@code /usr/local/service/flink/lib/} 下由集群提供，因此 lance-flink 本身
 * 无需绑定。
 *
 * <p><b>限制</b>：当前实现为整目录 <b>读时全量拷贝</b>，对小/中等规模 lance dataset
 * 有效；对超大 dataset 建议后续演进为 range-read 或增量同步策略。
 *
 * <p>本类线程安全；对同一源路径的并发 resolve 请求会串行化为一次下载。
 */
public final class LanceHadoopPathResolver {

    private static final Logger LOG = LoggerFactory.getLogger(LanceHadoopPathResolver.class);

    /** Lance native object_store 支持的 scheme，遇到这些直接透传。 */
    private static final Set<String> LANCE_NATIVE_SCHEMES = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(
                    "file", "s3", "s3+ddb", "gs", "az", "abfss", "oss", "cos",
                    "hf", "memory", "file+uring", "shared-memory", "file-object-store"
            )));

    /** 每个进程内所有已缓存的源 URI → 本地路径映射；同时兼作锁监视器。 */
    private static final ConcurrentHashMap<String, Object> CACHE_LOCKS = new ConcurrentHashMap<>();

    /**
     * Lance 的 latest 版本哨兵号（{@code u64::MAX - 1}）。Lance 在 {@code _versions/} 目录下
     * 用它标记"最新版本"的别名 manifest。该文件并非真实数据，Lance 在缺失时可通过扫描
     * {@code _versions/} 下的最大版本号确定 latest，因此对它读取失败时可安全跳过。
     * 部分 tbdsfs 后端会把这个哨兵 manifest 存坏（读时报 input/output error）。
     */
    private static final String LATEST_MANIFEST_SENTINEL = "18446744073709551614";

    private LanceHadoopPathResolver() {}

    /**
     * 解析 URI，如果需要则把远端 lance dataset 目录同步到本地缓存，返回可以直接
     * 传给 {@code Dataset.open()} 的路径。
     *
     * @param sourceUri     用户配置的 lance dataset URI，可能是
     *                      {@code /local/path}、{@code file:///...}、{@code s3://...}、
     *                      {@code tbdsfs://...}、{@code hdfs://...} 等
     * @param hadoopConf    Hadoop 配置（可为 {@code null}，此时使用默认 {@link Configuration}）
     * @param localCacheRoot 本地缓存根目录（可为 {@code null}，此时使用系统默认
     *                       {@code java.io.tmpdir/lance-hadoop-cache}）
     * @return 可直接传给 lance-core 的路径（本地路径或原路径透传）
     */
    public static String resolveForRead(String sourceUri, Configuration hadoopConf,
                                        String localCacheRoot) {
        if (sourceUri == null || sourceUri.isEmpty()) {
            return sourceUri;
        }
        // 规范化 URI：某些上游（如 Flink Table planner / Path 反序列化）会把
        // "scheme://authority/path" 收敛成 "scheme:/authority/path"（单斜杠），
        // 导致 URI.getAuthority() 变为 null，进而丢失 tbdsfs 需要的 name。
        // 这里通过把 "scheme:/xxx"（且不是 "scheme:///"）恢复为 "scheme://xxx"。
        sourceUri = normalizeUri(sourceUri);

        String scheme = extractScheme(sourceUri);
        if (scheme == null || LANCE_NATIVE_SCHEMES.contains(scheme)) {
            // 无 scheme（当作本地路径）或 lance 原生支持的 scheme，透传
            return sourceUri;
        }

        // 需要走 Hadoop FS 缓存
        Configuration conf = buildHadoopConfiguration(hadoopConf);
        applySchemeSpecificDefaults(conf, sourceUri, scheme);
        java.nio.file.Path cacheRoot = resolveCacheRoot(localCacheRoot);
        java.nio.file.Path targetDir = cacheRoot.resolve(sanitize(sourceUri));

        // 串行化并发访问：同一个 sourceUri 只下载一次
        Object lock = CACHE_LOCKS.computeIfAbsent(sourceUri, k -> new Object());
        synchronized (lock) {
            try {
                if (isCacheReady(targetDir)) {
                    LOG.info("Lance dataset {} already cached at {}, reuse local copy",
                            sourceUri, targetDir);
                } else {
                    LOG.info("Lance dataset {} not in native supported schemes ({}); "
                                    + "downloading via Hadoop FileSystem to {}",
                            sourceUri, scheme, targetDir);
                    downloadDirectory(new Path(sourceUri), targetDir, conf);
                }
                return "file://" + targetDir.toAbsolutePath();
            } catch (IOException e) {
                throw new UncheckedIOException(
                        "Failed to cache Lance dataset from " + sourceUri
                                + " to local " + targetDir
                                + ". Ensure the corresponding Hadoop FileSystem impl "
                                + "(e.g. tbdsfs-hadoop-*.jar) is available in classpath, "
                                + "and Kerberos credentials are valid.",
                        e);
            }
        }
    }

    /**
     * 构造 Hadoop {@link Configuration}：
     * <ol>
     *   <li>如果用户传入 {@code userConf}，以他为基础；否则新建一个（会自动
     *       加载 classpath 上的 {@code core-site.xml}）。</li>
     *   <li>扫描以 {@code lance.hadoop.} 为前缀的系统属性，去前缀后注入 Configuration。
     *       例如 {@code -Dlance.hadoop.fs.tbdsfs.impl=io.tbdsfs.TbdsFileSystem} 会被
     *       映射为 Hadoop 配置 {@code fs.tbdsfs.impl}。</li>
     *   <li>扫描以 {@code LANCE_HADOOP_} 为前缀的环境变量，以下划线为分隔符。
     *       例如 {@code LANCE_HADOOP_TBDSFS_META=zk://host:port} 会被映射为
     *       {@code tbdsfs.meta}。</li>
     * </ol>
     *
     * <p>处于 Flink TaskManager 环境中时，由于 YARN 容器不一定可以看到宿主机的
     * {@code core-site.xml}，上述两种方式作为兽底的配置注入通道。
     */
    static Configuration buildHadoopConfiguration(Configuration userConf) {
        Configuration conf = userConf != null ? userConf : new Configuration();

        // 显式加载 core-site.xml / hdfs-site.xml。在 Flink YARN TaskManager 容器内，
        // 由于 Flink 的 child-first 类加载器隔离，new Configuration() 未必能通过
        // context classloader 加载到宿主机的 core-site.xml，导致 tbdsfs.meta 等配置
        // 缺失（tbdsfs 的 Go 库会因此报 invalid uri 并 fatal）。这里按标准路径兜底加载。
        loadHadoopSiteXmls(conf);

        // 1. 从系统属性注入 lance.hadoop.* -> hadoop conf key
        java.util.Properties props = System.getProperties();
        for (String key : props.stringPropertyNames()) {
            if (key.startsWith("lance.hadoop.")) {
                String hadoopKey = key.substring("lance.hadoop.".length());
                String value = props.getProperty(key);
                conf.set(hadoopKey, value);
                LOG.info("Injected Hadoop conf from system property: {} = {}", hadoopKey, value);
            }
        }

        // 2. 从环境变量注入 LANCE_HADOOP_XXX -> hadoop conf key
        java.util.Map<String, String> env = System.getenv();
        for (java.util.Map.Entry<String, String> e : env.entrySet()) {
            if (e.getKey().startsWith("LANCE_HADOOP_")) {
                String hadoopKey = e.getKey().substring("LANCE_HADOOP_".length())
                        .toLowerCase(Locale.ROOT).replace('_', '.');
                String value = e.getValue();
                conf.set(hadoopKey, value);
                LOG.info("Injected Hadoop conf from environment variable: {} = {}", hadoopKey, value);
            }
        }

        return conf;
    }

    /**
     * 从 Flink 的全局配置构造 Hadoop {@link Configuration}。
     *
     * <p>Flink SQL 里通过 {@code SET 'flink.hadoop.xxx' = 'yyy'} 设置的配置会以
     * {@code flink.hadoop.} 前缀进入 Flink 的 {@code Configuration}。这里把这些
     * 前缀剥掉后注入 Hadoop {@code Configuration}（例如 {@code flink.hadoop.tbdsfs.meta}
     * → {@code tbdsfs.meta}）。
     *
     * <p>这解决了在 YARN TaskManager 容器内 {@code new Configuration()} 因类加载器
     * 隔离而加载不到宿主机的 {@code core-site.xml}（进而拿不到 {@code tbdsfs.meta}）
     * 的问题——tbdsfs 的 Go 库在 {@code meta} 为空时会 fallback 到把 name 当 URI，
     * 报 {@code invalid uri: /internal} 并直接 fatal 退出。
     *
     * @param flinkConf Flink 运行时配置（可为 {@code null}）
     * @return 注入了 {@code flink.hadoop.*} 配置的 Hadoop {@code Configuration}
     */
    public static Configuration buildHadoopConfigurationFromFlink(
            org.apache.flink.configuration.Configuration flinkConf) {
        Configuration conf = new Configuration();
        if (flinkConf == null) {
            return conf;
        }
        for (java.util.Map.Entry<String, String> e : flinkConf.toMap().entrySet()) {
            String key = e.getKey();
            if (key != null && key.startsWith("flink.hadoop.")) {
                String hadoopKey = key.substring("flink.hadoop.".length());
                conf.set(hadoopKey, e.getValue());
                LOG.info("Injected Hadoop conf from Flink config: {} = {}", hadoopKey, e.getValue());
            }
        }
        return conf;
    }

    /**
     * 针对特定 scheme 应用兜底默认值：
     * <ul>
     *   <li>{@code tbdsfs://<authority>/...}：tbdsfs 的 {@code TbdsFileSystemImpl.initialize}
     *       要求 {@code tbdsfs.name} 配置存在。如果用户未显式配置且 URI 的 authority 非空，
     *       则自动把 authority 作为 {@code tbdsfs.name} 注入。同时保证
     *       {@code fs.tbdsfs.impl} 存在（默认 {@code io.tbdsfs.TbdsFileSystem}）。</li>
     * </ul>
     *
     * <p>该方法只做"缺省填充"，永远不会覆盖用户已经显式设置的值。
     */
    static void applySchemeSpecificDefaults(Configuration conf, String sourceUri, String scheme) {
        if (conf == null || scheme == null) return;
        if (!"tbdsfs".equals(scheme)) return;

        // 1. 确保 fs.tbdsfs.impl 存在
        if (conf.get("fs.tbdsfs.impl") == null) {
            conf.set("fs.tbdsfs.impl", "io.tbdsfs.TbdsFileSystem");
            LOG.info("Applied default fs.tbdsfs.impl = io.tbdsfs.TbdsFileSystem");
        }

        // 2. 如果 tbdsfs.name 未配置，从 URI authority 推导
        if (conf.get("tbdsfs.name") == null) {
            try {
                URI u = new URI(sourceUri);
                String authority = u.getAuthority();
                if (authority != null && !authority.isEmpty()) {
                    conf.set("tbdsfs.name", authority);
                    LOG.info("Applied default tbdsfs.name = {} (from URI authority)", authority);
                }
            } catch (URISyntaxException ignore) {
                // fall through; tbdsfs will raise its own error if truly missing
            }
        }
    }

    /**
     * 规范化 URI：如果输入形如 {@code scheme:/authority/path}（单斜杠，authority 与 path
     * 之间没有明确分隔），将其转换为 {@code scheme://authority/path}（双斜杠）。
     * 常见触发场景是 Flink Table 反序列化 URI 时 {@code new Path(str)} 会丢失一个斜杠。
     * 幂等：对已经形如 {@code scheme://...} 或 {@code scheme:///...}（无 authority）的
     * URI 不做任何改动。
     */
    static String normalizeUri(String uri) {
        if (uri == null) return null;
        int colon = uri.indexOf(':');
        if (colon <= 0 || colon >= uri.length() - 1) return uri;
        String rest = uri.substring(colon + 1);
        // 如果已经是 "//..."（含空 authority 的 "///..."），保持原样
        if (rest.startsWith("//")) return uri;
        // 只处理 "scheme:/xxx" 且第 2 个字符不是 '/'（否则已经是双斜杠了）
        if (rest.startsWith("/") && !rest.startsWith("//")) {
            String scheme = uri.substring(0, colon);
            return scheme + ":/" + rest;   // 把 "scheme:/xxx" 变为 "scheme://xxx"
        }
        return uri;
    }

    /** 提取 scheme。返回小写 scheme，如果没有 scheme 返回 {@code null}。 */
    static String extractScheme(String uri) {
        try {
            URI u = new URI(uri);
            String s = u.getScheme();
            return s == null ? null : s.toLowerCase(Locale.ROOT);
        } catch (URISyntaxException e) {
            // 非标 URI（比如 Windows 路径 C:\），当作本地路径
            return null;
        }
    }

    static boolean isNativeSupportedScheme(String scheme) {
        return scheme != null && LANCE_NATIVE_SCHEMES.contains(scheme.toLowerCase(Locale.ROOT));
    }

    /**
     * 显式加载 Hadoop 的 {@code core-site.xml} / {@code hdfs-site.xml}。
     * 用于兜底 Flink YARN 容器内 {@code new Configuration()} 因类加载器隔离而
     * 加载不到宿主机 site 文件的问题。
     */
    private static void loadHadoopSiteXmls(Configuration conf) {
        java.nio.file.Path confDir = resolveHadoopConfDir();
        if (confDir == null) {
            return;
        }
        addSiteXmlIfExists(conf, confDir.resolve("core-site.xml"));
        addSiteXmlIfExists(conf, confDir.resolve("hdfs-site.xml"));
    }

    private static void addSiteXmlIfExists(Configuration conf, java.nio.file.Path siteXml) {
        if (Files.isRegularFile(siteXml)) {
            conf.addResource(new Path(siteXml.toUri()));
            LOG.info("Explicitly loaded Hadoop site config: {}", siteXml);
        }
    }

    /** 探测 Hadoop 配置目录：HADOOP_CONF_DIR → 系统属性 → TBDS 标准路径。 */
    private static java.nio.file.Path resolveHadoopConfDir() {
        String env = System.getenv("HADOOP_CONF_DIR");
        if (env != null && !env.isEmpty()) {
            return java.nio.file.Paths.get(env);
        }
        String prop = System.getProperty("hadoop.conf.dir");
        if (prop != null && !prop.isEmpty()) {
            return java.nio.file.Paths.get(prop);
        }
        java.nio.file.Path standard = java.nio.file.Paths.get("/usr/local/service/hadoop/etc/hadoop");
        if (Files.isDirectory(standard)) {
            return standard;
        }
        return null;
    }

    private static java.nio.file.Path resolveCacheRoot(String userSpecified) {
        if (userSpecified != null && !userSpecified.isEmpty()) {
            return java.nio.file.Paths.get(userSpecified);
        }
        String tmpDir = System.getProperty("java.io.tmpdir", "/tmp");
        return java.nio.file.Paths.get(tmpDir, "lance-hadoop-cache");
    }

    /** 把 URI 转为可作为文件系统目录名的安全字符串。 */
    private static String sanitize(String uri) {
        return uri.replaceAll("[^A-Za-z0-9._-]", "_");
    }

    /** 缓存目录是否已经就绪（存在 {@code _versions/} 子目录，Lance dataset 的标志）。 */
    private static boolean isCacheReady(java.nio.file.Path targetDir) {
        if (!Files.isDirectory(targetDir)) return false;
        java.nio.file.Path versionsDir = targetDir.resolve("_versions");
        return Files.isDirectory(versionsDir);
    }

    /**
     * 递归把 {@code srcDir} 下的所有文件同步到 {@code destDir}（本地目录）。
     * 保留相对目录结构。
     */
    private static void downloadDirectory(Path srcDir, java.nio.file.Path destDir,
                                          Configuration conf) throws IOException {
        FileSystem fs = srcDir.getFileSystem(conf);
        LOG.info("Resolved Hadoop FileSystem for {}: uri={}, impl={}",
                srcDir, fs.getUri(), fs.getClass().getName());
        FileStatus rootStatus = fs.getFileStatus(srcDir);
        if (!rootStatus.isDirectory()) {
            throw new IOException("Lance dataset path is not a directory: " + srcDir);
        }
        Files.createDirectories(destDir);

        long fileCount = 0L;
        long byteCount = 0L;
        int skippedSentinelFiles = 0;
        RemoteIterator<LocatedFileStatus> it = fs.listFiles(srcDir, true);
        while (it.hasNext()) {
            LocatedFileStatus st = it.next();
            if (st.isDirectory()) continue;
            Path srcFile = st.getPath();
            String rel = relativize(srcDir, srcFile);
            java.nio.file.Path dst = destDir.resolve(rel);
            Files.createDirectories(dst.getParent());
            // 使用 copyToLocalFile：deleteSource=false, useRawLocalFileSystem=true 避免 checksum 校验
            try {
                fs.copyToLocalFile(false, srcFile, new Path(dst.toUri()), true);
                fileCount++;
                byteCount += st.getLen();
            } catch (IOException e) {
                // latest 哨兵 manifest（u64::MAX-1）只是 latest 版本的别名，损坏/不可读
                // 不影响 Lance 通过扫描 _versions 目录确定 latest，因此安全跳过。
                if (isLatestSentinelManifest(rel)) {
                    LOG.warn("Skipping unreadable latest-sentinel manifest {} (tbdsfs read error: {})",
                            srcFile, e.getMessage());
                    skippedSentinelFiles++;
                } else {
                    throw e;
                }
            }
        }
        LOG.info("Downloaded lance dataset from {} to {}: {} files, {} bytes (skipped {} unreadable latest-sentinel manifests)",
                srcDir, destDir, fileCount, byteCount, skippedSentinelFiles);
    }

    /**
     * 判断相对路径 {@code rel} 是否指向 Lance 的 latest 哨兵 manifest
     * （即 {@code _versions/18446744073709551614.manifest}）。
     */
    private static boolean isLatestSentinelManifest(String rel) {
        if (rel == null) return false;
        int slash = rel.lastIndexOf('/');
        String name = slash >= 0 ? rel.substring(slash + 1) : rel;
        return name.equals(LATEST_MANIFEST_SENTINEL + ".manifest");
    }

    /**
     * 计算 {@code srcFile} 相对 {@code srcDir} 的路径字符串，永远使用 '/' 分隔符。
     */
    private static String relativize(Path srcDir, Path srcFile) {
        String base = srcDir.toUri().getPath();
        String full = srcFile.toUri().getPath();
        if (!base.endsWith("/")) base = base + "/";
        if (full.startsWith(base)) {
            return full.substring(base.length());
        }
        // Fallback：直接取文件名
        return srcFile.getName();
    }
}
