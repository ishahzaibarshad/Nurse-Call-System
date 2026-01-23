module.exports = {
  content: [
    "./templates/**/*.html",
    "./**/*.html",        // ✅ catches any html outside templates too
    "./static/**/*.js",
    "./**/*.py",
  ],
  theme: {
    extend: {},
  },
  plugins: [],
}
