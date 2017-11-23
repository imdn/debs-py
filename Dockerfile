FROM python:3.6.3-stretch

# Set the working directory to /app
WORKDIR /debs2017

# Copy the current directory contents into the container at /app
ADD . /debs2017

# Install any needed packages specified in requirements.txt
RUN pip install --trusted-host pypi.python.org -r py_packages.txt

# Run app.py when the container launches
CMD [ "python", "main.py" "--testrun" ]
