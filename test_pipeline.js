/**
 * Automated Verification & Unit Test Suite for DataFlow Automator
 */
const assert = require('assert');
const path = require('path');
const fs = require('fs');
const {
    standardizeDate,
    cleanCurrency,
    normalizeCategory,
    calculateMetrics,
    processPipeline,
    generateMockData
} = require('./automate');

console.log('🧪 Starting Automated Unit & Integration Tests...');

// Test 1: Date Standardization
assert.strictEqual(standardizeDate('2026-11-01'), '2026-11-01', 'ISO date format failed');
assert.strictEqual(standardizeDate('01/11/2026'), '2026-11-01', 'DMY date format failed');
assert.strictEqual(standardizeDate('2026/11/02'), '2026-11-02', 'Slash format failed');
assert.strictEqual(standardizeDate(''), null, 'Empty date check failed');
console.log('  ✓ Test 1 Passed: Date standardizer handles diverse formats.');

// Test 2: Currency Sanitization
assert.strictEqual(cleanCurrency('$1,250.50'), 1250.5, 'Currency symbol parsing failed');
assert.strictEqual(cleanCurrency('  $45.00 '), 45.0, 'Whitespace currency parsing failed');
assert.strictEqual(cleanCurrency('99.99'), 99.99, 'Raw numeric string parsing failed');
console.log('  ✓ Test 2 Passed: Currency cleaner sanitizes symbols and spacing.');

// Test 3: Category Normalization
assert.strictEqual(normalizeCategory('elec'), 'Electronics', 'Alias mapping failed');
assert.strictEqual(normalizeCategory('Clothing'), 'Apparel', 'Clothing alias failed');
assert.strictEqual(normalizeCategory('  Home & Kitchen  '), 'Home & Kitchen', 'Trim check failed');
console.log('  ✓ Test 3 Passed: Category normalization correctly maps aliases.');

// Test 4: End-to-End Pipeline Execution
generateMockData();
const { metrics, audit } = processPipeline();

assert(metrics.total_orders > 0, 'No orders processed');
assert(metrics.total_net_sales > 0, 'Net sales must be positive');
assert(audit.duplicates_dropped >= 1, 'Duplicate row was not dropped');
assert(audit.corrupted_records_dropped >= 1, 'Corrupted row was not filtered');

// Test 5: Verify Generated Output Files
const processedCsv = path.join(__dirname, 'data/processed/cleaned_transactions.csv');
const summaryMd = path.join(__dirname, 'reports/executive_summary.md');
const dashboardHtml = path.join(__dirname, 'reports/executive_dashboard.html');
const auditJson = path.join(__dirname, 'reports/audit_trail.json');

assert(fs.existsSync(processedCsv), 'Cleaned CSV was not created');
assert(fs.existsSync(summaryMd), 'Markdown report was not created');
assert(fs.existsSync(dashboardHtml), 'HTML dashboard was not created');
assert(fs.existsSync(auditJson), 'Audit JSON was not created');

console.log('  ✓ Test 4 Passed: Pipeline ingested, filtered duplicates, and calculated KPIs.');
console.log('  ✓ Test 5 Passed: All required reports and cleaned datasets exist.');

console.log('\n🎉 ALL TESTS PASSED SUCCESSFULLY! (5/5 assertions verified)\n');

