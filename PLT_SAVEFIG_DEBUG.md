# plt.savefig() Debugging Guide

## The Problem

When you see:
```python
plot_path = '/tmp/tmp0imnreir/confusion_matrix.png'
plt.savefig(plot_path)
```

## Current Fix Applied

The code now:
1. ✅ Creates temp directory with `tempfile.mkdtemp()`
2. ✅ Ensures directory exists with `os.makedirs(plot_dir, exist_ok=True)`
3. ✅ Verifies directory is writable before saving
4. ✅ Wraps `plt.savefig()` in try/except with detailed error logging
5. ✅ Verifies file was created after saving
6. ✅ Checks file size to ensure it's not empty

## Debug Output

With debug logging enabled, you'll see:
```
✅ Directory verified: /tmp/tmp0imnreir (exists: True, writable: True)
✅ Plot saved successfully: /tmp/tmp0imnreir/confusion_matrix.png
✅ Plot file verified: /tmp/tmp0imnreir/confusion_matrix.png (size: 12345 bytes)
```

If it fails, you'll see exactly where:
```
❌ Failed to save plot to /tmp/tmp0imnreir/confusion_matrix.png: [error]
   Directory exists: True/False
   Directory writable: True/False
   Full absolute path: /tmp/tmp0imnreir/confusion_matrix.png
```
