# Security Guidelines

## Credential Management

### Local Development

1. **NEVER commit `.env` files to version control**
   - The `.env` file contains sensitive credentials
   - It is already listed in `.gitignore` to prevent accidental commits
   - Use `.env.example` as a template

2. **Setup your local environment**
   ```bash
   # Copy the example file
   cp .env.example .env

   # Edit .env with your credentials
   # The file will be ignored by git
   ```

3. **Use Gmail App Passwords**
   - DO NOT use your regular Gmail password
   - Create an App Password: https://support.google.com/accounts/answer/185833
   - Store the App Password in your `.env` file

### GitHub Actions / CI/CD

1. **Use GitHub Secrets for credentials**
   - Never hardcode credentials in workflow files
   - Use encrypted secrets: Settings → Secrets and variables → Actions

2. **Required secrets:**
   - `SMTP_HOST` - Your SMTP server (e.g., smtp.gmail.com)
   - `SMTP_USER` - Your email address
   - `SMTP_PASS` - Gmail App Password (NOT regular password)
   - `NOTIFICATION_EMAIL` - Email for notifications

## What's Protected

The following files/patterns are excluded from version control:

- `.env` - Contains SMTP credentials
- `*.db` - SQLite database files (too large, contains processed data)
- `*.log` - Log files may contain sensitive information
- `__pycache__/` - Python cache files

## Checking for Exposed Secrets

If you accidentally commit secrets:

1. **Immediately rotate/change the exposed credentials**
2. **Remove from git history:**
   ```bash
   git filter-branch --force --index-filter \
     "git rm --cached --ignore-unmatch .env" \
     --prune-empty --tag-name-filter cat -- --all
   ```
3. **Force push to remote (requires coordination with team):**
   ```bash
   git push origin --force --all
   ```

## Database Security

- SQLite database files are excluded from git (too large, binary format)
- Database is recreated on each environment from raw data
- No sensitive/personal data is stored in the database
- All data comes from public CAISO reports

## Reporting Security Issues

If you discover a security vulnerability, please report it by:
1. Creating a private security advisory in GitHub
2. NOT creating a public issue
3. Emailing the maintainer directly

## Security Checklist

Before committing:
- [ ] No credentials in code or config files
- [ ] `.env` file not staged for commit
- [ ] No API keys or tokens in files
- [ ] Secrets use environment variables or GitHub Secrets
- [ ] Database files not committed
- [ ] Log files cleaned/excluded
