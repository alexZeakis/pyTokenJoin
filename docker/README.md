# Docker Instructions

To build the image run:
```
docker build -t pytokenjoin .
```

To create the container run:
```
docker run pytokenjoin <token> <endpoint_url> <task_exec_id>
```

Please examine the `template_input.json` to understand the required parameters.
