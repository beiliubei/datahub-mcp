import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from functools import wraps
from typing import (
    Any,
    Dict,
    Optional,
    AsyncIterator,
    Callable,
    TypeVar,
    Awaitable,
)

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI
from loguru import logger
from mcp.server.fastmcp import FastMCP, Context

"""
Datahub MCP Integration

"""

# Load environment variables from .env file
load_dotenv()

# Constants
DATAHUB_BASE_URL = os.getenv("DATAHUB_BASE_URL", "http://localhost:8088")
DATAHUB_USERNAME = os.getenv("DATAHUB_USERNAME")
DATAHUB_PASSWORD = os.getenv("DATAHUB_PASSWORD")
ACCESS_TOKEN_STORE_PATH = os.path.join(os.path.dirname(__file__), ".datahub_token")

# Initialize FastAPI app for handling additional web endpoints if needed
app = FastAPI(title="Datahub MCP Server")


@dataclass
class DatahubContext:
    """Typed context for the Datahub MCP server"""

    client: httpx.AsyncClient
    base_url: str
    access_token: Optional[str] = None
    csrf_token: Optional[str] = None
    app: FastAPI = None


def load_stored_token() -> Optional[str]:
    """Load stored access token if it exists"""
    try:
        if os.path.exists(ACCESS_TOKEN_STORE_PATH):
            with open(ACCESS_TOKEN_STORE_PATH, "r") as f:
                return f.read().strip()
    except Exception:
        return None
    return None


def save_access_token(token: str):
    """Save access token to file"""
    try:
        with open(ACCESS_TOKEN_STORE_PATH, "w") as f:
            f.write(token)
    except Exception as e:
        print(f"Warning: Could not save access token: {e}")


@asynccontextmanager
async def Datahub_lifespan(server: FastMCP) -> AsyncIterator[DatahubContext]:
    """Manage application lifecycle for Datahub integration"""
    print("Initializing Datahub context...")
    logger.add("file_{time}.log")

    # Create HTTP client
    client = httpx.AsyncClient(base_url=DATAHUB_BASE_URL, timeout=30.0)

    # Create context
    ctx = DatahubContext(client=client, base_url=DATAHUB_BASE_URL, app=app)

    # Try to load existing token
    stored_token = load_stored_token()
    if stored_token:
        ctx.access_token = stored_token
        # Set the token in the client headers
        client.headers.update({"Authorization": f"Bearer {stored_token}"})
        print("Using stored access token")

        # Verify token validity
        try:
            response = await client.get("/v3/entity/dataset")
            if response.status_code != 200:
                print(
                    f"Stored token is invalid (status {response.status_code}). Will need to re-authenticate."
                )
                logger.info("Re-authenticating with stored access token")
                ctx.access_token = None
                client.headers.pop("Authorization", None)
        except Exception as e:
            print(f"Error verifying stored token: {e}")
            logger.info("Error verifying stored token")
            ctx.access_token = None
            client.headers.pop("Authorization", None)

    try:
        yield ctx
    finally:
        # Cleanup on shutdown
        print("Shutting down Datahub context...")
        await client.aclose()


# Initialize FastMCP server with lifespan and dependencies
mcp = FastMCP(
    "Datahub",
    lifespan=Datahub_lifespan,
    dependencies=["fastapi", "uvicorn", "python-dotenv", "httpx"],
)

# Type variables for generic function annotations
T = TypeVar("T")
R = TypeVar("R")

# ===== Helper Functions and Decorators =====


def requires_auth(
    func: Callable[..., Awaitable[Dict[str, Any]]],
) -> Callable[..., Awaitable[Dict[str, Any]]]:
    """Decorator to check authentication before executing a function"""

    @wraps(func)
    async def wrapper(ctx: Context, *args, **kwargs) -> Dict[str, Any]:
        Datahub_ctx: DatahubContext = ctx.request_context.lifespan_context

        if not Datahub_ctx.access_token:
            return {"error": "Not authenticated. Please authenticate first."}

        return await func(ctx, *args, **kwargs)

    return wrapper


def handle_api_errors(
    func: Callable[..., Awaitable[Dict[str, Any]]],
) -> Callable[..., Awaitable[Dict[str, Any]]]:
    """Decorator to handle API errors in a consistent way"""

    @wraps(func)
    async def wrapper(ctx: Context, *args, **kwargs) -> Dict[str, Any]:
        try:
            return await func(ctx, *args, **kwargs)
        except Exception as e:
            # Extract function name for better error context
            function_name = func.__name__
            return {"error": f"Error in {function_name}: {str(e)}"}

    return wrapper


async def make_api_request(
    ctx: Context,
    method: str,
    endpoint: str,
    data: Dict[str, Any] = None,
    params: Dict[str, Any] = None,
    auto_refresh: bool = True,
) -> Dict[str, Any]:
    """
    Helper function to make API requests to Datahub

    Args:
        ctx: MCP context
        method: HTTP method (get, post, put, delete)
        endpoint: API endpoint (without base URL)
        data: Optional JSON payload for POST/PUT requests
        params: Optional query parameters
        auto_refresh: Whether to auto-refresh token on 401
    """
    Datahub_ctx: DatahubContext = ctx.request_context.lifespan_context
    client = Datahub_ctx.client

    async def make_request() -> httpx.Response:
        headers = {}

        if method.lower() == "get":
            return await client.get(endpoint, params=params)
        elif method.lower() == "post":
            return await client.post(
                endpoint, json=data, params=params, headers=headers
            )
        elif method.lower() == "put":
            return await client.put(endpoint, json=data, headers=headers)
        elif method.lower() == "delete":
            return await client.delete(endpoint, headers=headers)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")

    # Use auto_refresh if requested
    response = (
        await make_request()
    )

    if response.status_code not in [200, 201]:
        return {
            "error": f"API request failed: {response.status_code} - {response.text}"
        }

    return response.json()


@mcp.tool()
@requires_auth
@handle_api_errors
async def Datahub_dataset_list(ctx: Context, count: int) -> Dict[str, Any]:
    """
    List datasets in Datahub
    :param ctx:
    :param count:
    :return:
    """
    return await make_api_request(ctx, "get", f"/v3/entity/dataset?systemMetadata=false&includeSoftDelete=false&skipCache=false&aspects=datasetKey&count={count}&sortCriteria=urn&sortOrder=ASCENDING")


@mcp.tool()
@requires_auth
@handle_api_errors
async def Datahub_dataset_get_by_urn(
    ctx: Context, dashboard_urn: str
) -> Dict[str, Any]:
    """
    Get a specific dashboard by URN
    :param ctx:
    :param dashboard_urn:
    :return:
    """
    return await make_api_request(ctx, "get", f"/v3/entity/dataset/{dashboard_urn}")


if __name__ == "__main__":
    print("Starting Datahub MCP server...")
    mcp.run()
