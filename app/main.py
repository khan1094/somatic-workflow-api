from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.responses import FileResponse
from app.argo_client import submit_workflow, get_workflow, list_workflows, delete_workflow
from app.kube_client import get_nodes
from app.utils import summarize_results, get_result_file_path
import os
import shutil
import gzip

app = FastAPI(title="Somatic Workflow API")

UPLOAD_DIR = os.getenv("UPLOAD_DIR", "/refdata/samples")


# -----------------------------------
# Health Check
# -----------------------------------
@app.get("/")
def health():
    return {"status": "ok"}


# -----------------------------------
# Create Workflow (Upload + Reference Mode)
# -----------------------------------
@app.post("/workflows")
async def create_workflow(
    sample_vcf: str = Form(None),
    node: str = Form(None),
    file: UploadFile = File(None)
):
    try:

        # ----------------------------
        # Mode A — Upload
        # ----------------------------
        if file and file.filename:

            if not file.filename.endswith(".vcf.gz"):
                raise HTTPException(status_code=400, detail="File must be .vcf.gz")

            os.makedirs(UPLOAD_DIR, exist_ok=True)
            upload_path = os.path.join(UPLOAD_DIR, file.filename)

            # Save file
            with open(upload_path, "wb") as buffer:
                shutil.copyfileobj(file.file, buffer)

            # Validate gzip
            try:
                with gzip.open(upload_path, "rt") as f:
                    f.readline()
            except Exception:
                os.remove(upload_path)
                raise HTTPException(status_code=400, detail="Invalid gzip file")

            wf = submit_workflow(file.filename, node)

        # ----------------------------
        # Mode B — Reference
        # ----------------------------
        elif sample_vcf:

            ref_path = os.path.join(UPLOAD_DIR, sample_vcf)

            if not os.path.exists(ref_path):
                raise HTTPException(status_code=400, detail="Referenced file not found in /refdata/samples")

            wf = submit_workflow(sample_vcf, node)

        else:
            raise HTTPException(status_code=400, detail="Provide file or sample_vcf")

        return {
            "workflow_name": wf["metadata"]["name"],
            "status": wf.get("status", {}).get("phase", "Pending"),
            "node": node
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -----------------------------------
# List Workflows
# -----------------------------------
@app.get("/workflows")
def get_all_workflows():
    try:
        data = list_workflows()
        items = data.get("items", [])

        return [
            {
                "name": wf["metadata"]["name"],
                "status": wf.get("status", {}).get("phase", "Pending"),
                "created_at": wf["metadata"].get("creationTimestamp")
            }
            for wf in items
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -----------------------------------
# Get Workflow Status
# -----------------------------------
@app.get("/workflows/{workflow_name}")
def get_workflow_status(workflow_name: str):
    try:
        wf = get_workflow(workflow_name)

        return {
            "name": wf["metadata"]["name"],
            "status": wf.get("status", {}).get("phase"),
            "started_at": wf.get("status", {}).get("startedAt"),
            "finished_at": wf.get("status", {}).get("finishedAt"),
            "progress": wf.get("status", {}).get("progress")
        }
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))


# -----------------------------------
# Cancel Workflow
# -----------------------------------
@app.delete("/workflows/{workflow_name}")
def cancel_workflow(workflow_name: str):
    try:
        delete_workflow(workflow_name)
        return {"message": "Workflow deleted"}
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))


# -----------------------------------
# List Nodes
# -----------------------------------
@app.get("/nodes")
def list_nodes():
    try:
        return get_nodes()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -----------------------------------
# Get Workflow Results Summary
# -----------------------------------
@app.get("/workflows/{workflow_name}/results")
def get_workflow_results(workflow_name: str):

    try:
        wf = get_workflow(workflow_name)
        status = wf.get("status", {}).get("phase")

        if status != "Succeeded":
            return {
                "workflow_name": workflow_name,
                "status": status,
                "message": "Workflow not completed yet"
            }

        file_path, summary = summarize_results(workflow_name)

        return {
            "workflow_name": workflow_name,
            "status": status,
            "output_file": file_path,
            "summary": summary
        }

    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -----------------------------------
# Download Raw Results
# -----------------------------------
@app.get("/workflows/{workflow_name}/download")
def download_results(workflow_name: str):

    file_path = get_result_file_path(workflow_name)

    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Results file not found")

    return FileResponse(
        path=file_path,
        media_type="text/tab-separated-values",
        filename=f"{workflow_name}.tsv"
    )
