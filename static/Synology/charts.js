
const charts = {};

document.addEventListener('DOMContentLoaded', function() {
    console.log('DOM loaded');
    
    // 从JSON文件加载数据
    const jsonFile = document.body.dataset.jsonFile;
    
    if (!jsonFile) {
        console.error('No JSON file specified in data-json-file attribute');
        return; // 停止执行
    }

    fetch(jsonFile)
        .then(response => response.json())
        .then(data => {
            const tabButtons = document.getElementById('tabButtons');
            const tabContents = document.getElementById('tabContents');
            let isFirst = true;
            
            // 生成标签按钮和内容
            for (const [category, categoryData] of Object.entries(data)) {
                const categoryId = category.replace(/ /g, '_');
                
                // 创建标签按钮
                const button = document.createElement('button');
                button.className = `tab-button ${isFirst ? 'active' : ''}`;
                button.setAttribute('data-tab', categoryId);
                button.textContent = category;
                button.addEventListener('click', function(event) {
                    openTab(event, categoryId);
                });
                tabButtons.appendChild(button);
                
                // 创建标签内容区域
                const tabContent = document.createElement('div');
                tabContent.id = categoryId;
                tabContent.className = `tab-content ${isFirst ? 'active' : ''}`;
                
                // 遍历每个数据集并创建图表和表格
                for (const [key, value] of Object.entries(categoryData)) {
                    if (!value || value.length === 0) continue;
                    
                    const keyId = key.replace(/ /g, '_');
                    const chartContainerId = `chart-container-${categoryId}-${keyId}`;
                    const chartId = `chart-${categoryId}-${keyId}`;
                    const tableId = `table-${categoryId}-${keyId}`;
                    const controlId = `control-${categoryId}-${keyId}`;
                    const titleId = `title-${categoryId}-${keyId}`;
                    
                    // 创建标题
                    const title = document.createElement('h3');
                    title.className = 'chart-title';
                    title.id = titleId;
                    title.textContent = key;
                    tabContent.appendChild(title);
                    
                    // 创建控制区域
                    const controlArea = document.createElement('div');
                    controlArea.className = 'control-area';
                    controlArea.id = controlId;
                    
                    // 表格切换
                    const tableToggleLabel = document.createElement('label');
                    const tableToggle = document.createElement('input');
                    tableToggle.type = 'checkbox';
                    tableToggle.className = 'table-toggle';
                    tableToggleLabel.appendChild(tableToggle);
                    tableToggleLabel.appendChild(document.createTextNode(' 显示表格'));
                    controlArea.appendChild(tableToggleLabel);
                    
                    // 图表切换
                    const chartToggleLabel = document.createElement('label');
                    const chartToggle = document.createElement('input');
                    chartToggle.type = 'checkbox';
                    chartToggle.className = 'chart-toggle';
                    chartToggle.checked = true;
                    chartToggleLabel.appendChild(chartToggle);
                    chartToggleLabel.appendChild(document.createTextNode(' 显示图表'));
                    controlArea.appendChild(chartToggleLabel);
                    
                    // 列选择下拉框（如果不是堆叠图表）
                    if (!key.endsWith('_stacked')) {
                        const columnSelect = document.createElement('select');
                        columnSelect.className = 'column-select';
                        
                        // 添加表头为选项（跳过第一列）
                        const headers = Object.keys(value[0]);
                        headers.forEach((header, index) => {
                            if (index > 0) {
                                const option = document.createElement('option');
                                option.value = index;
                                option.textContent = header;
                                columnSelect.appendChild(option);
                            }
                        });
                        
                        controlArea.appendChild(columnSelect);
                    }
                    
                    tabContent.appendChild(controlArea);
                    
                    // 创建图表容器
                    const chartContainer = document.createElement('div');
                    chartContainer.className = 'chart-container';
                    chartContainer.id = chartContainerId;
                    
                    const canvas = document.createElement('canvas');
                    canvas.id = chartId;
                    chartContainer.appendChild(canvas);
                    tabContent.appendChild(chartContainer);
                    
                    // 创建表格容器
                    const tableContainer = document.createElement('div');
                    tableContainer.className = 'table-container';
                    tableContainer.style.display = 'none'; // 默认隐藏
                    
                    const table = document.createElement('table');
                    table.className = 'data-table';
                    table.id = tableId;
                    
                    // 创建表头
                    const thead = document.createElement('thead');
                    const headerRow = document.createElement('tr');
                    
                    const headers = Object.keys(value[0]);
                    headers.forEach(header => {
                        const th = document.createElement('th');
                        th.textContent = header;
                        headerRow.appendChild(th);
                    });
                    
                    thead.appendChild(headerRow);
                    table.appendChild(thead);
                    
                    // 创建表格内容
                    const tbody = document.createElement('tbody');
                    value.forEach(row => {
                        const tr = document.createElement('tr');
                        
                        Object.values(row).forEach(cellValue => {
                            const td = document.createElement('td');
                            // 如果是数字，则保留两位小数
                            if (!isNaN(parseFloat(cellValue)) && isFinite(cellValue)) {
                                td.textContent = parseFloat(cellValue).toFixed(2);
                            } else {
                                td.textContent = cellValue;
                            }
                            tr.appendChild(td);
                        });
                        
                        tbody.appendChild(tr);
                    });
                    
                    table.appendChild(tbody);
                    tableContainer.appendChild(table);
                    tabContent.appendChild(tableContainer);
                }
                
                tabContents.appendChild(tabContent);
                isFirst = false;
            }
            
            // 添加事件监听器
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
                        updateChart(tableId, chartId, this.value);
                    });
                }
            });
            
            // 创建所有图表
            setTimeout(createAllCharts, 100);
            
            // 窗口大小改变时重新调整图表大小
            window.addEventListener('resize', function() {
                Object.values(charts).forEach(chart => {
                    if (chart) chart.resize();
                });
            });
        })
        .catch(error => console.error('Error loading data:', error));
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
                backgroundColor: `hsl(${index * 60}, 70%, 50%)`,
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
        const values = data.map(row => {
            const value = row[columnIndex];
            return isNaN(parseFloat(value)) ? 0 : parseFloat(value);
        });
        
        const backgroundColors = values.map((val, idx) => {
            const hue = 200;
            const saturation = 70;
            const lightness = Math.max(40, 85 - (val / Math.max(...values) * 45));
            return `hsl(${hue}, ${saturation}%, ${lightness}%)`;
        });
        
        if (charts[chartId]) {
            charts[chartId].destroy();
        }
        
        charts[chartId] = new Chart(chartCanvas, {
            type: 'bar',
            data: {
                labels: labels,
                datasets: [{
                    label: headers[columnIndex], 
                    data: values,
                    backgroundColor: backgroundColors,
                    borderColor: 'rgba(54, 162, 235, 1)',
                    borderWidth: 1
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    y: {
                        beginAtZero: true,
                        max: Math.max(...values) * 1.1,
                        ticks: {
                            callback: function(value) {
                                return value >= 1000 ? value.toLocaleString() : value.toFixed(2);
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
