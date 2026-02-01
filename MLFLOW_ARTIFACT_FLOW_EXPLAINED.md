# MLflow Artifact Flow - Complete Explanation

## Understanding the Confusion: `/mlflow` in Logs

### The Key Insight

When you see `/mlflow` in logs or MLflow UI, it's showing the **SERVER-SIDE storage path**, NOT where your code writes files.

### Complete Flow Diagram

```
┌─────────────────────────────────────────────────────────────┐
│ YOUR CODE (Running on HOST)                                 │
└─────────────────────────────────────────────────────────────┘
                    │
                    │ 1. Create temp directory
                    ▼
         ┌──────────────────────┐
         │ tempfile.mkdtemp()   │
         │ → /tmp/tmpABC123/    │ ✅ HOST filesystem
         └──────────────────────┘
                    │
                    │ 2. Write file to temp dir
                    ▼
         ┌──────────────────────┐
         │ plt.savefig()         │
         │ → /tmp/tmpABC123/    │ ✅ HOST filesystem
         │   confusion_matrix.png│
         └──────────────────────┘
                    │
                    │ 3. Log to MLflow
                    ▼
         ┌──────────────────────┐
         │ mlflow.log_artifact() │
         │ (MLflow Python Client)│
         └──────────────────────┘
                    │
                    │ 4. Upload via HTTP
                    ▼
┌─────────────────────────────────────────────────────────────┐
│ MLFLOW SERVER (Running in CONTAINER)                       │
└─────────────────────────────────────────────────────────────┘
                    │
                    │ 5. Store artifact
                    ▼
         ┌──────────────────────┐
         │ /mlflow/artifacts/   │ ✅ CONTAINER filesystem
         │   1/                  │    (This is what you see in logs!)
         │   run_id/             │
         │   artifacts/          │
         │   confusion_matrix.png│
         └──────────────────────┘
```

## What You See vs. What Actually Happens

### In Your Code Logs:
```python
logger.info(f"Logged confusion matrix artifact: /tmp/tmpABC123/confusion_matrix.png")
# ✅ This is CORRECT - shows where code wrote the file (HOST)
```

### In MLflow UI/Logs:
```
Artifact URI: /mlflow/artifacts/1/abc123def456/artifacts/confusion_matrix.png
# ✅ This is CORRECT - shows where MLflow stores it (CONTAINER)
```

## Fixed Issues

### Issue 1: Code Writing to `/mlflow` (WRONG)
**Before:**
```python
art_dir = '/mlflow/artifacts'  # ❌ Container path - doesn't exist on host!
plot_path = os.path.join(art_dir, "confusion_matrix.png")
plt.savefig(plot_path)  # FAILS: FileNotFoundError
```

**After:**
```python
tmpdir = tempfile.mkdtemp()  # ✅ Creates temp dir on host
plot_path = os.path.join(tmpdir, "confusion_matrix.png")
plt.savefig(plot_path)  # ✅ Works: Directory exists
mlflow.log_artifact(plot_path)  # ✅ MLflow uploads to server
```

### Issue 2: Code Writing to `./mlruns` (WRONG)
**Before:**
```python
artifact_path = f"./mlruns/{model_name}_test_predictions.csv"  # ❌ Wrong path
```

**After:**
```python
tmpdir = tempfile.mkdtemp()  # ✅ Temp dir on host
artifact_path = os.path.join(tmpdir, f"{model_name}_test_predictions.csv")  # ✅ Correct
```

## Summary Table

| Location | Path | Who Creates It | When | Status |
|----------|------|----------------|------|--------|
| **Code writes** | `/tmp/tmpXXX/file.png` | Your Python code | During training | ✅ Temp, cleaned up |
| **MLflow stores** | `/mlflow/artifacts/.../file.png` | MLflow server | After upload | ✅ Permanent |
| **MLflow UI shows** | `/mlflow/artifacts/.../file.png` | MLflow UI | When viewing | ✅ Correct |

**Key Takeaway**: `/mlflow` in MLflow logs/UI is the SERVER storage path (correct). Your code writes to `/tmp/tmpXXX/` (also correct). Both are correct - they're just different locations in the flow!
