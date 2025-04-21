from setuptools import setup, find_packages

setup(
    name="metrics_etl",
    version="0.1",
    packages=find_packages(include=['metrics_etl', 'metrics_etl.*']),
    package_dir={'': '.'},
    install_requires=[
        "requests==2.32.2",
        "python-dotenv==1.0.0",
        "gspread==5.11.0",
        "oauth2client==4.1.3",
        "certifi==2024.2.2",
        "supabase==1.0.3",
        "google-api-python-client==2.118.0",
        "google-auth-httplib2==0.2.0",
        "google-auth-oauthlib==1.2.0",
    ],
    python_requires='>=3.8',
) 