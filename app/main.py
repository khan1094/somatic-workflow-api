from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.responses import FileResponse
from app.argo_client import submit_workflow, get_workflow, list_workflows, delete_workflow
from app.kube_client import get_nodes
from app.utils import summarize_results, get_result_file_path
import os
import shutil
import gzip
from datetime import datetime

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

        if file and file.filename:

            if not file.filename.endswith(".vcf.gz"):
                raise HTTPException(status_code=400, detail="File must be .vcf.gz")

            os.makedirs(UPLOAD_DIR, exist_ok=True)
            upload_path = os.path.join(UPLOAD_DIR, file.filename)

            with open(upload_path, "wb") as buffer:
                shutil.copyfileobj(file.file, buffer)

            try:
                with gzip.open(upload_path, "rt") as f:
                    f.readline()
            except Exception:
                os.remove(upload_path)
                raise HTTPException(status_code=400, detail="Invalid gzip file")

            wf = submit_workflow(file.filename, node)

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
# List Workflows (Filtering + Pagination)
# -----------------------------------
@app.get("/workflows")
def get_all_workflows(
    status: str = None,
    limit: int = 20,
    offset: int = 0
):
    try:
        data = list_workflows()
        items = data.get("items", [])

        if status:
            items = [
                wf for wf in items
                if wf.get("status", {}).get("phase") == status
            ]

        items = items[offset:offset + limit]

        response = []

        for wf in items:
            metadata = wf.get("metadata", {})
            wf_status = wf.get("status", {})
            spec = wf.get("spec", {})

            parameters = spec.get("arguments", {}).get("parameters", [])
            sample_vcf = next(
                (p.get("value") for p in parameters if p.get("name") == "sample-vcf"),
                None
            )

            node = spec.get("nodeSelector", {}).get("kubernetes.io/hostname")

            response.append({
                "name": metadata.get("name"),
                "status": wf_status.get("phase"),
                "created_at": metadata.get("creationTimestamp"),
                "started_at": wf_status.get("startedAt"),
                "finished_at": wf_status.get("finishedAt"),
                "input_sample": sample_vcf,
                "node": node
            })

        return response

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -----------------------------------
# Get Workflow Status (ENHANCED)
# -----------------------------------
@app.get("/workflows/{workflow_name}")
def get_workflow_status(workflow_name: str):
    try:
        wf = get_workflow(workflow_name)

        metadata = wf.get("metadata", {})
        wf_status = wf.get("status", {})
        spec = wf.get("spec", {})

        phase = wf_status.get("phase")
        created_at = metadata.get("creationTimestamp")
        started_at = wf_status.get("startedAt")
        finished_at = wf_status.get("finishedAt")
        progress = wf_status.get("progress")

        # Extract input sample
        parameters = spec.get("arguments", {}).get("parameters", [])
        sample_vcf = next(
            (p.get("value") for p in parameters if p.get("name") == "sample-vcf"),
            None
        )

        # Extract node
        node = spec.get("nodeSelector", {}).get("kubernetes.io/hostname")

        # Duration calculation
        duration = None
        if started_at and finished_at:
            try:
                start = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
                end = datetime.fromisoformat(finished_at.replace("Z", "+00:00"))
                duration = (end - start).total_seconds()
            except Exception:
                duration = None

        # Error extraction (if failed)
        error_message = None
        if phase in ["Failed", "Error"]:
            error_message = wf_status.get("message")

        return {
            "name": metadata.get("name"),
            "status": phase,
            "created_at": created_at,
            "started_at": started_at,
            "finished_at": finished_at,
            "duration_seconds": duration,
            "progress": progress,
            "input_sample": sample_vcf,
            "node": node,
            "error_message": error_message
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