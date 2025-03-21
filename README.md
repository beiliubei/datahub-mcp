# datahub-mcp


## Running Your Server
### Development Mode
The fastest way to test and debug your server is with the MCP Inspector:

```shell
mcp dev main.py

# Add dependencies
mcp dev main.py --with pandas --with numpy

# Mount local code
mcp dev main.py --with-editable .
```

## Setup Instructions


1**Clone This Repository**

   Clone this repository to your local machine.

2. **Configure Environment Variables**

   Create a `.env` file in the root directory with your Datahub credentials:
   ```
   DATAHUB_BASE_URL=http://localhost:8088  # Change to your Datahub URL
   DATAHUB_USERNAME=your_username
   DATAHUB_PASSWORD=your_password
   ```

3. **Install Dependencies**

   ```bash
   uv pip install .
   ```

4. **Install MCP Config for Claude**

   To use with Claude Desktop app:
   ```bash
   mcp install main.py
   ```

## Usage with Claude

After setup, you can interact with your Datahub instance via Claude using natural language requests. Here are some examples:

### Dashboard Management

- **View datasets**: "Show me all my Datahub datasets"
- **Get dataset details**: "Show me the details of dataset with urn urn:li:dataset:(urn:li:dataPlatform:trino,dataplatform.dm_app.qrcode,PROD)"
