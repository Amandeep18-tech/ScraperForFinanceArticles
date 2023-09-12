FROM public.ecr.aws/lambda/python:3.11 as build

RUN yum install -y unzip && \
    curl -Lo "/tmp/chromedriver.zip" "https://chromedriver.storage.googleapis.com/114.0.5735.90/chromedriver_linux64.zip" && \
    curl -Lo "/tmp/chrome-linux.zip" "https://www.googleapis.com/download/storage/v1/b/chromium-browser-snapshots/o/Linux_x64%2F1135561%2Fchrome-linux.zip?alt=media" && \
    unzip /tmp/chromedriver.zip -d /opt/ && \
    unzip /tmp/chrome-linux.zip -d /opt/

FROM public.ecr.aws/lambda/python:3.11

RUN yum install atk cups-libs gtk3 libXcomposite alsa-lib \
    libXcursor libXdamage libXext libXi libXrandr libXScrnSaver \
    libXtst pango at-spi2-atk libXt xorg-x11-server-Xvfb \
    xorg-x11-xauth dbus-glib dbus-glib-devel -y

COPY --from=build /opt/chrome-linux /opt/chrome
COPY --from=build /opt/chromedriver /opt/

COPY requirements.txt ${LAMBDA_TASK_ROOT}
# Copy all your function code and files to the working directory
COPY financial_express_lambda.py ${LAMBDA_TASK_ROOT}

COPY constants.py ${LAMBDA_TASK_ROOT}

COPY sqs_utils.py ${LAMBDA_TASK_ROOT}

RUN yum install -y gcc
# Copy requirements.txt first
RUN pip install --upgrade pip
# Install Python dependencies from requirements.txt
RUN pip install -r requirements.txt

# Specify the command to run your Python code
CMD [ "financial_express_lambda.lambda_handler" ]





