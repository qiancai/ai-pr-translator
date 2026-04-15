# 图片处理功能说明 (Image Processing Feature)

## 概述

此脚本现在支持自动处理源语言 PR 中的图片文件变更，并将相同的变更应用到目标语言 PR 中。

## 功能特性

### 1. 支持的图片格式
- PNG (`.png`)
- JPEG (`.jpg`, `.jpeg`)
- GIF (`.gif`)
- SVG (`.svg`)
- WebP (`.webp`)
- BMP (`.bmp`)
- ICO (`.ico`)

### 2. 支持的操作类型

#### 新增图片 (Added Images)
- 从源语言 repo 下载新增的图片
- 将图片保存到目标语言 repo 的相应位置
- 自动创建必要的目录结构

#### 修改图片 (Modified Images)
- 从源语言 repo 下载更新后的图片
- 覆盖目标语言 repo 中的旧图片
- 保持文件路径不变

#### 删除图片 (Deleted Images)
- 从目标语言 repo 中删除对应的图片文件
- 清理不再需要的图片资源

#### 重命名图片 (Renamed Images)
- 自动处理为"删除旧文件"+"新增新文件"的组合操作

## 工作流程

### 1. 分析阶段 (`diff_analyzer.py`)

在 `analyze_source_changes()` 函数中，脚本会：
- 识别 PR 中所有的图片文件
- 根据文件状态分类：`added`、`modified`、`removed`、`renamed`
- 返回三个列表：`added_images`、`modified_images`、`deleted_images`

### 2. 处理阶段 (`image_processor.py`)

主工作流会调用 `process_all_images()` 函数，按顺序执行：
1. 删除已删除的图片
2. 添加新增的图片
3. 更新修改的图片

## 使用方法

### 自动处理
图片处理功能已集成到主工作流中，无需额外配置。运行主脚本时会自动处理图片：

```bash
python scripts/main_workflow_local.py
# 或
python scripts/main_workflow.py
```

### 输出示例

```
🖼️  Found 3 image files

🔍 Analyzing image: media/example.png
   ➕ Detected new image: media/example.png

🔍 Analyzing image: media/diagram.svg
   🔄 Detected modified image: media/diagram.svg

🔍 Analyzing image: media/old-screenshot.png
   🗑️  Detected deleted image: media/old-screenshot.png

📊 Summary:
   🖼️  Added images: 1 images
   🖼️  Modified images: 1 images
   🖼️  Deleted images: 1 images

🖼️  Step 3.5: Processing images...

🖼️  Processing 1 deleted images...
   ✅ Deleted image: /path/to/docs-cn/media/old-screenshot.png

🖼️  Processing 1 newly added images...
   ✅ Saved image to: /path/to/docs-cn/media/example.png

🖼️  Processing 1 modified images...
   ✅ Updated image: /path/to/docs-cn/media/diagram.svg

✅ IMAGE PROCESSING COMPLETED
```

## 代码架构

### 主要模块

1. **`image_processor.py`** - 图片处理核心模块
   - `is_image_file()` - 判断文件是否为图片
   - `download_image_from_source()` - 从源 repo 下载图片
   - `process_added_images()` - 处理新增图片
   - `process_modified_images()` - 处理修改图片
   - `process_deleted_images()` - 处理删除图片
   - `process_all_images()` - 统一处理所有图片操作

2. **`diff_analyzer.py`** - 扩展了图片检测功能
   - 在 `analyze_source_changes()` 中添加了图片文件识别
   - 返回值新增：`added_images`, `modified_images`, `deleted_images`

3. **`main_workflow.py` & `main_workflow_local.py`** - 集成图片处理流程
   - 在 Step 3.5 中调用 `process_all_images()`
   - 在最终摘要中显示图片处理统计

## 技术细节

### 图片下载
- 使用 GitHub API 的 `get_contents()` 方法获取图片的二进制内容
- 通过 `decoded_content` 属性获取解码后的图片数据

### 文件操作
- 图片以二进制模式写入 (`'wb'`)
- 自动创建必要的目录结构
- 支持覆盖已存在的文件（用于修改操作）

### 线程安全
- 使用 `thread_safe_print()` 确保并发环境下的输出正确
- 支持多线程处理多个文件

## 注意事项

1. **权限要求**
   - 需要有效的 GitHub token
   - Token 需要有读取源 repo 和写入目标 repo 的权限

2. **文件路径**
   - 图片路径相对于 repo 根目录
   - 目标路径自动匹配源路径

3. **错误处理**
   - 下载失败时会显示错误信息并跳过该文件
   - 不会因单个图片失败而中断整个流程

4. **大文件支持**
   - GitHub API 对单个文件大小有限制（通常为 1MB）
   - 超大图片文件可能需要使用 Git LFS

## 未来改进方向

- [ ] 支持 Git LFS 管理的大型图片文件
- [ ] 添加图片压缩优化选项
- [ ] 支持批量下载以提高性能
- [ ] 添加图片验证（检查文件完整性）
- [ ] 支持图片格式转换（可选）

## 故障排查

### 常见问题

**Q: 图片下载失败**
```
❌ Error downloading image from source: 404
```
**A**: 检查图片路径是否正确，确认文件在源 PR 的 head commit 中存在

**Q: 目标目录创建失败**
```
❌ Error saving image: Permission denied
```
**A**: 检查目标路径的写入权限，确保本地 repo 路径配置正确

**Q: 图片未被识别**
```
📄 Found 0 image files
```
**A**: 确认文件扩展名在支持列表中，检查 `IMAGE_EXTENSIONS` 配置

## 贡献

欢迎提交 Issue 和 Pull Request 来改进图片处理功能！
