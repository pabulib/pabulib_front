# Contributing to Pabulib Frontend

Thank you for your interest in contributing to the Pabulib Frontend! This document provides guidelines and information for contributors.

## How to Contribute

### Reporting Issues

If you find a bug or have a feature request:

1. Check if the issue already exists in the [Issues](https://github.com/pabulib/pabulib_front/issues) section
2. If not, create a new issue using the appropriate template
3. Provide as much detail as possible, including:
   - Steps to reproduce (for bugs)
   - Expected vs actual behavior
   - Your environment (OS, browser, etc.)

### Submitting Pull Requests

1. **Fork the repository** and create a new branch from `main`
2. **Set up the development environment**:
   ```bash
   git clone https://github.com/yourusername/pabulib_front.git
   cd pabulib_front
   docker compose up --build
   ```
3. **Make your changes** and test them locally
4. **Follow the coding standards**:
   - Use clear, descriptive commit messages
   - Follow existing code style and conventions
   - Add comments for complex logic
5. **Test your changes**:
   - Ensure the app runs without errors
   - Test both UI functionality and data processing
   - Verify database operations work correctly
6. **Submit a pull request** with:
   - Clear description of changes
   - Reference to related issues (if any)
   - Screenshots (for UI changes)

## Development Setup

### Prerequisites
- Docker and Docker Compose
- Git

### Local Development
```bash
# Clone the repository
git clone https://github.com/pabulib/pabulib_front.git
cd pabulib_front

# Start the development environment
docker compose up --build

# Access the application
# App: http://localhost:5050
# Database UI: http://localhost:8080
```

### Project Structure
- `app/` - Main Flask application code
- `pb_files/` - Participatory budgeting data files
- `scripts/` - Database and utility scripts
- `templates/` - HTML templates
- `static/` - CSS, JS, and other static assets

### Adding New .pb Files
After adding new .pb files to the `pb_files/` directory:
```bash
docker compose exec web python -m scripts.db_refresh
```

## Code Style Guidelines

- **Python**: Follow PEP 8 conventions
- **HTML/CSS**: Use consistent indentation (2 spaces)
- **JavaScript**: Use modern ES6+ syntax where possible
- **Comments**: Write clear, concise comments for complex logic
- **Commit messages**: Use imperative mood (e.g., "Add feature" not "Added feature")

## Types of Contributions

We welcome contributions in these areas:

- **Bug fixes**: Fix existing functionality
- **Feature enhancements**: Improve existing features
- **New features**: Add new functionality for .pb file analysis
- **Documentation**: Improve README, comments, or guides
- **UI/UX improvements**: Enhance user interface and experience
- **Performance optimizations**: Improve app speed and efficiency
- **Data processing**: Enhance .pb file parsing and validation

## Questions?

If you have questions about contributing:

1. Check existing [Issues](https://github.com/pabulib/pabulib_front/issues) and [Pull Requests](https://github.com/pabulib/pabulib_front/pulls)
2. Create a new issue with the "question" label
3. Reach out to the maintainers

## License

By contributing to this project, you agree that your contributions will be licensed under the same license as the project.