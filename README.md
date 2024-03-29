# github-crawler

Extract GitHub repositories metadata and README content.

STEPS:
1. environment SETUP and package installation

### Virtual env

    ```sh
    python3 -m venv env
    source env/bin/activate
    pip install --upgrade pip
    pip install -r requirements.txt
    # when finish using
    deactivate 
    ```

### Conda

    ```sh
    conda env create -f conda.yaml
    conda activate crawler
    # when finish using
    conda deactivate
    ```

2. Update the `.env` file with the correct params

    ```sh
    cp .env.example .env
    code .env
    ```

3. Run the following scripts:

    i. `python crawl_repos.py <topic-name> <stars-size>` to crawl all the repos with the topic <topic-name> and stars greater or equal <stars-size>. If omitted will consider 0+ stars.

    ii. `python get_contributors.py ` to crawl all the user who contributed the crawled repo from step 3.i

    iii. `python get_stargazers.py ` to crawl all the users who starred the crawled repo from step 3.i
