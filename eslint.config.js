// Minimal ESLint v9 flat config. Pulls in the recommended rule sets from
// `@eslint/js` and `typescript-eslint`, and turns off rules that conflict
// with the snake_case naming convention used throughout this codebase.
const js = require('@eslint/js');
const tseslint = require('typescript-eslint');

module.exports = [
  {
    ignores: ['build/**', 'node_modules/**', 'coverage/**', '.nyc_output/**'],
  },
  js.configs.recommended,
  ...tseslint.configs.recommended,
  {
    files: ['**/*.ts'],
    languageOptions: {
      ecmaVersion: 2020,
      sourceType: 'module',
    },
    rules: {
      '@typescript-eslint/no-explicit-any': 'off',
      '@typescript-eslint/no-unused-vars': [
        'warn',
        { argsIgnorePattern: '^_', varsIgnorePattern: '^_' },
      ],
      '@typescript-eslint/no-require-imports': 'off',
      'no-empty': ['error', { allowEmptyCatch: true }],
      'no-case-declarations': 'off',
    },
  },
  {
    // chai's BDD chain (`expect(x).to.be.true`) reads as an unused
    // expression to ESLint. Don't fight it inside the test tree.
    files: ['test/**/*.ts'],
    rules: {
      '@typescript-eslint/no-unused-expressions': 'off',
    },
  },
];
