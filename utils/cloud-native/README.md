# Folder contents

- `system-diagnostics-cn.sh` can be used in Linux environment (or in WSL/MSYS on Windows) to collect system diagnostics bundle wihtout access to Yellowbrick Manager Web UI. A knowledge base [article](https://support.yellowbrick.com/hc/en-us/articles/55437554667539-Yellowbrick-Cloud-Native-System-Diagnostics-101) covers its usage.
- `yb-ecr-inspector.py` finds and reports all ECR repositories with specified prefix that have more than one image. Found repos/images are sorted by their last access time, making it easy to see which ones should likely be purged.

