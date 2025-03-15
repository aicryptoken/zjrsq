const charts = {};

// 自定义颜色方案
const stackedColors = ['#A6CEE3', '#1F78B4', '#B2DF8A', '#33A02C', '#FB9A99', '#E31A1C', '#FDBF6F', '#FF7F00', '#CAB2D6', '#6A3D9A', '#FFFF99', '#B15928'];
const barColors = ['#1F78B4', '#A6CEE3', '#33A02C', '#B2DF8A'];

document.addEventListener('DOMContentLoaded', function() {
    console.log('DOM loaded');
    
    document.querySelector('.tab-content').classList.add('active');
    document.querySelector('.tab-button').classList.add('active');

    document.querySelectorAll('.tab-button').forEach(button => {
        button.addEventListener('click', function(event) {
            const tabName = this.getAttribute('data-tab');
            openTab(event, tabName);
        });
    });

    console.log('Starting chart creation');
    setTimeout(function() {
        document.querySelectorAll('.column-select').forEach(select => {
            if (select && select.options.length > 0) {
                select.selectedIndex = 0;
            }
        });
        createAllCharts();
    }, 100);

    document.querySelectorAll('.table-toggle').forEach(toggle => {
        toggle.addEventListener('change', function() {
            const controlId = this.closest('.control-area').id;
            const tableContainer = document.getElementById('table-' + controlId.replace('control-', '')).closest('.table-container');
            tableContainer.style.display = this.checked ? 'block' : 'none';
        });
    });

    document.querySelectorAll('.chart-toggle').forEach(toggle => {
        toggle.addEventListener('change', function() {
            const controlId = this.closest('.control-area').id;
            const chartId = 'chart-container-' + controlId.replace('control-', '');
            const chartContainer = document.getElementById(chartId);
            chartContainer.style.display = this.checked ? 'block' : 'none';
            
            if (this.checked && charts[chartId.replace('container-', '')]) {
                setTimeout(() => {
                    charts[chartId.replace('container-', '')].resize();
                }, 50);
            }
        });
    });

    document.querySelectorAll('.column-select').forEach(select => {
        if (select) {
            select.addEventListener('change', function() {
                const controlId = this.closest('.control-area').id;
                const tableId = 'table-' + controlId.replace('control-', '');
                const chartId = 'chart-' + controlId.replace('control-', '');
                console.log('Column selected:', this.value);
                updateChart(tableId, chartId, parseInt(this.value));
            });
        }
    });

    window.addEventListener('resize', function() {
        Object.values(charts).forEach(chart => {
            if (chart) chart.resize();
        });
    });
});

function openTab(evt, tabName) {
    var i, tabcontent, tabbuttons;
    tabcontent = document.getElementsByClassName("tab-content");
    for (i = 0; i < tabcontent.length; i++) {
        tabcontent[i].classList.remove("active");
    }
    tabbuttons = document.getElementsByClassName("tab-button");
    for (i = 0; i < tabbuttons.length; i++) {
        tabbuttons[i].classList.remove("active");
    }
    document.getElementById(tabName).classList.add("active");
    evt.currentTarget.classList.add("active");
    setTimeout(() => {
        document.querySelectorAll(`#${tabName} canvas`).forEach(canvas => {
            const chartId = canvas.id;
            if (charts[chartId]) {
                charts[chartId].resize();
            }
        });
    }, 50);
}

function createAllCharts() {
    document.querySelectorAll('.data-table').forEach(table => {
        const tableId = table.id;
        const chartId = tableId.replace('table', 'chart');
        const chartCanvas = document.getElementById(chartId);
        const controlArea = document.getElementById(tableId.replace('table', 'control'));
        const columnSelect = controlArea ? controlArea.querySelector('.column-select') : null;
        
        if (chartCanvas) {
            console.log('Creating chart for:', chartId);
            const columnIndex = columnSelect && columnSelect.options.length > 0 ? parseInt(columnSelect.value) : 0;
            updateChart(tableId, chartId, columnIndex);
        } else {
            console.error('Chart canvas not found for:', chartId);
        }
    });
}

function updateChart(tableId, chartId, columnIndex) {
    const table = document.getElementById(tableId);
    const chartCanvas = document.getElementById(chartId);
    const titleElement = document.getElementById('title-' + tableId.replace('table-', ''));

    if (!table || !chartCanvas || !titleElement) {
        console.error('Required elements not found:', tableId, chartId);
        return;
    }

    // 判断图表类型并清理标题
    let tableTitle = titleElement.textContent;
    const isStacked = tableTitle.endsWith('_stacked');
    const isBar = tableTitle.endsWith('_bar') || !isStacked;
    tableTitle = tableTitle.replace('_bar', '').replace('_stacked', '');

    // 提取表格数据
    const headers = Array.from(table.querySelectorAll('thead th')).map(th => th.textContent);
    const rows = Array.from(table.querySelectorAll('tbody tr'));
    const data = rows.map(row => {
        return Array.from(row.querySelectorAll('td')).map(td => td.textContent);
    });

    if (isStacked) {
        // 100%堆叠柱状图
        const labels = data.map(row => row[0]);
        const datasets = headers.slice(1).map((header, index) => {
            const values = data.map(row => {
                const value = parseFloat(row[index + 1]);
                return isNaN(value) ? 0 : value;
            });
            return {
                label: header,
                data: values,
                backgroundColor: stackedColors[index % stackedColors.length],
            };
        });

        // 计算百分比
        const totals = data.map((_, rowIndex) => {
            return datasets.reduce((sum, dataset) => sum + dataset.data[rowIndex], 0);
        });
        datasets.forEach(dataset => {
            dataset.data = dataset.data.map((value, index) => {
                return totals[index] === 0 ? 0 : (value / totals[index]) * 100;
            });
        });

        if (charts[chartId]) {
            charts[chartId].destroy();
        }

        charts[chartId] = new Chart(chartCanvas, {
            type: 'bar',
            data: {
                labels: labels,
                datasets: datasets
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    x: {
                        stacked: true,
                        ticks: { maxRotation: 90, minRotation: 90, font: { size: 11 } }
                    },
                    y: {
                        stacked: true,
                        max: 100,
                        beginAtZero: true,
                        ticks: {
                            callback: value => `${value}%`,
                            font: { size: 11 }
                        }
                    }
                },
                plugins: {
                    legend: { display: true, position: 'top' },
                    title: {
                        display: true,
                        text: `${tableTitle} (百分比)`,
                        font: { size: 16, weight: 'bold' },
                        padding: { top: 10, bottom: 20 }
                    },
                    tooltip: {
                        callbacks: {
                            label: context => `${context.dataset.label}: ${context.parsed.y.toFixed(2)}%`
                        }
                    }
                }
            }
        });
    } else if (isBar) {
        // 普通柱状图
        const labels = data.map(row => row[0]);
        const datasets = [];
        const selectedColumns = columnIndex === 0 ? Array.from({length: headers.length - 1}, (_, i) => i) : [columnIndex];
        
        selectedColumns.forEach((colIndex, index) => {
            const values = data.map(row => {
                const value = parseFloat(row[colIndex]);
                return isNaN(value) ? 0 : value;
            });
            datasets.push({
                label: headers[colIndex],
                data: values,
                backgroundColor: barColors[index % barColors.length],
            });
        });

        if (charts[chartId]) {
            charts[chartId].destroy();
        }

        charts[chartId] = new Chart(chartCanvas, {
            type: 'bar',
            data: {
                labels: labels,
                datasets: datasets
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    y: {
                        beginAtZero: true,
                        ticks: {
                            callback: function(value, index, values) {
                                const maxValue = Math.max(...values.map(t => t.value));
                                return maxValue < 10 ? value.toFixed(1) : value.toLocaleString();
                            },
                            font: { size: 11 }
                        }
                    },
                    x: {
                        ticks: { maxRotation: 90, minRotation: 90, font: { size: 11 } }
                    }
                },
                plugins: {
                    legend: { display: false },
                    title: {
                        display: true,
                        text: tableTitle,
                        font: { size: 16, weight: 'bold' },
                        padding: { top: 10, bottom: 20 }
                    },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                let label = context.dataset.label || '';
                                if (label) label += ': ';
                                if (context.parsed.y !== null) {
                                    label += context.parsed.y.toLocaleString(undefined, {
                                        minimumFractionDigits: 2,
                                        maximumFractionDigits: 2
                                    });
                                }
                                return label;
                            }
                        }
                    }
                }
            }
        });
    }
    console.log('Chart created successfully for:', chartId);
}
