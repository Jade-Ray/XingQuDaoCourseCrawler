# XingQuDao Course Crawler

正式版爬虫应用：输入兴趣岛点播页面 URL，自动抓取 m3u8/ts 视频流并合并为 MP4。

## 功能

- 配置类和爬虫类模块化拆分
- 仅保留一个执行流程：点播页 -> ts 分片 -> mp4
- Cookie 通过文件导入，不在代码中明文保存
- 使用 `pathlib` 处理文件路径和目录
- 异常体系化：登录态、INIT_DATA、m3u8、下载、合并分别抛出异常
- 异步下载后自动检查分片完整性，缺失分片自动重试下载
- 下载按课程自动分目录：`topicId_topicName/ts/*.ts`，合并视频保存在课程目录
- 支持断点续传：已下载分片会跳过，失败分片自动重试，采用 `.part` 原子写入
- 每次任务生成 `manifest.json`，记录下载进度、输出路径和失败原因
- 支持转码预设：`quality`（不压缩）、`balanced`（均衡）、`size`（体积优先）
- 自动化测试（pytest + pytest-asyncio）

## 项目结构

```text
.
├── src/
│   └── xingqudao_crawler/
│       ├── __init__.py
│       ├── cli.py
│       ├── config.py
│       ├── crawler.py
│       └── exceptions.py
├── tests/
│   ├── test_config.py
│   └── test_crawler.py
├── main.py
├── pyproject.toml
├── .gitignore
└── GetVideoIDwithLogin.ipynb   # 原始测试 notebook（可保留）
```

## 安装

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -e .[test]
```

## 准备 Cookie 文件

在项目根目录创建 `cookies.txt`，文件内容为完整 Cookie 字符串（一行即可），例如：

```text
rsessionId=...; uid=...; userId=...; ...
```

## 运行

```bash
python main.py "https://m1.nicegoods.cn/financial/open-live?topicId=..."
```

下载产物目录示例：

```text
download/
└── 109421404274_课程标题/
  ├── manifest.json
  ├── ts/
  │   ├── 00000.ts
  │   ├── 00001.ts
  │   └── ...
  └── 课程标题_1080p.mp4
```

可选参数：

```bash
python main.py "<vod_page_url>" \
  --cookie-file cookies.txt \
  --save-dir download \
  --quality 1080p \
  --transcode-mode quality \
  --timeout 30 \
  --max-concurrency 20 \
  --retry-rounds 3
```

转码模式说明：

- `quality`：`-c copy`，最快且不压缩，体积最大
- `balanced`：H.264 + `crf=25`，画质和体积折中
- `size`：H.264 + `crf=30`，体积最小，画质损失更明显

## 测试

```bash
pytest -q
```

## 注意事项

- 需要本机安装 `ffmpeg` 并可在命令行直接调用。
- 如登录态失效，请更新 `cookies.txt`。
- 默认合并完成后会清理 ts 分片；加 `--keep-ts` 可保留分片。
