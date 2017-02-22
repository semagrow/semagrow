var gulp = require('gulp'),
    browserify = require('browserify'),
    connect = require('gulp-connect'),
    concat = require('gulp-concat'),
    embedlr = require('gulp-embedlr'),
    jsValidate = require('gulp-jsvalidate'),
    source = require('vinyl-source-stream'),
    uglify = require("gulp-uglify"),
    rename = require("gulp-rename"),
    streamify = require('gulp-streamify'),
    buffer = require('vinyl-buffer'),
    exorcist = require('exorcist'),
    shim = require('browserify-shim'),
    notify = require('gulp-notify'),
    runSequence = require('run-sequence').use(gulp),
    sourcemaps = require('gulp-sourcemaps');
    sass = require('gulp-sass'),
    cssImport = require('gulp-cssimport'),
    minifyCSS = require('gulp-minify-css');


var paths = {
    yasqe: 'node_modules/yasgui-yasqe/',
    yasr: 'node_modules/yasgui-yasr/',
    queryviz: 'semagrow_site/',
    style: ['scss/scoped.scss', 'scss/global.scss'],
    bundleDir: "dist",
    bundleName: "yasgui",
    docDir: "doc",
    webappresDir: "../main/webapp/resources/"
};

gulp.task('default', ['browserify']);


gulp.task('browserifyWithDeps', function() {
    var bundler = browserify({entries: ["./entry.js"],standalone: "YASGUI", debug: true});

    return bundler
        .transform({global:true}, shim)
        .bundle()
        .pipe(exorcist(paths.bundleDir + '/' + paths.bundleName + '.js.map'))
        .pipe(source(paths.bundleName + '.js'))
        .pipe(gulp.dest(paths.bundleDir))
        .pipe(rename(paths.bundleName + '.min.js'))
        .pipe(buffer())
        .pipe(sourcemaps.init({
            loadMaps: true,
            debug:true,
        }))
        .pipe(uglify({
            compress: {
                //disable the compressions. Otherwise, breakpoints in minified files don't work (sourcemaped lines get offset w.r.t. original)
                //minified files does increase from 457 to 459 kb, but can live with that
                negate_iife: false,
                sequences: false
            }
        }))
        .pipe(sourcemaps.write('./'))
        .pipe(gulp.dest(paths.bundleDir));
});
gulp.task('browserify', function() {
    runSequence('browserifyWithDeps', 'makeBundledCopy', 'makeCss', 'copyResources');
});
gulp.task('makeBundledCopy', function() {
    //keep copy with 'bundled' in name as well (for backwards compatability)
    gulp.src(paths.bundleDir + '/' + paths.bundleName + '.min.js')
        .pipe(rename(paths.bundleName + '.bundled.min.js'))
        .pipe(gulp.dest(paths.bundleDir));
    gulp.src(paths.bundleDir + '/' + paths.bundleName + '.js')
        .pipe(rename(paths.bundleName + '.bundled.js'))
        .pipe(gulp.dest(paths.bundleDir));
});

gulp.task('copyResources', function() { 
    gulp.src(paths.bundleDir + '/' + paths.bundleName + '.min.js')
        .pipe(gulp.dest(paths.webappresDir + "/js"));
    gulp.src(paths.bundleDir + '/' + paths.bundleName + '.min.css')
        .pipe(gulp.dest(paths.webappresDir +"/styles"));
});

/**
 * Faster, because we don't minify, and include source maps in js file (notice we store it with .min.js extension, so we don't have to change the index.html file for debugging)
 */
gulp.task('browserifyForDebug', function() {
    var bundler = browserify({entries: ["./src/entry.js"],standalone: "YASGUI", debug: true});

    return bundler
        .transform({global:true}, shim)
        .bundle()
        .on("error", notify.onError(function(error) {
            return error.message;
        }))
        .pipe(source(paths.bundleName + '.min.js'))
        .pipe(embedlr())
        .pipe(gulp.dest(paths.bundleDir))
        .pipe(connect.reload());
});


gulp.task('makeCss', function() {
    return gulp.src(paths.style)
        .pipe(cssImport())//needed, because css files are not -actually- imported by sass, but remain as css @import statement...
        .pipe(sass())
        .on("error", notify.onError(function(error) {
            return error.message;
        }))
        .pipe(concat(paths.bundleName + '.css'))
        .pipe(gulp.dest(paths.bundleDir))
        .pipe(minifyCSS({
            //the minifyer does not work well with lines including a comment. e.g.
            ///* some comment */ }
            //is completely removed (including the final bracket)
            //So, disable the 'advantaced' feature. This only makes the minified file 100 bytes larger
            noAdvanced: true,
        }))
        .pipe(rename(paths.bundleName + '.min.css'))
        .pipe(gulp.dest(paths.bundleDir))
        .pipe(connect.reload());
})

