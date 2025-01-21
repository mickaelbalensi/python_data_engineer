# scripts/setup-git.sh
#!/bin/bash
# Setup Git configuration if environment variables are provided
if [ ! -z "$GIT_USERNAME" ] && [ ! -z "$GIT_EMAIL" ]; then
    git config --global user.name "$GIT_USERNAME"
    git config --global user.email "$GIT_EMAIL"
    echo "Git configured with user: $GIT_USERNAME ($GIT_EMAIL)"
fi

# Ensure SSH directory has correct permissions
if [ -d "/root/.ssh" ]; then
    chmod 700 /root/.ssh
    chmod 600 /root/.ssh/*
fi