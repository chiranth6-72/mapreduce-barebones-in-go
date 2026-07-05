// Global state variables
let currentTab = 'cluster';
let selectedJobID = null;
let lastState = null;

// Initialize Clock
setInterval(() => {
    const clock = document.getElementById('system-clock');
    if (clock) {
        const now = new Date();
        clock.textContent = now.toTimeString().split(' ')[0];
    }
}, 1000);

// Switch Tabs
function switchTab(tab) {
    currentTab = tab;
    ['cluster', 'pipeline', 'storage'].forEach(t => {
        const btn = document.getElementById(`tab-btn-${t}`);
        const sect = document.getElementById(`tab-${t}`);
        if (t === tab) {
            btn.className = "border-indigo-500 text-indigo-400 border-b-2 py-4 px-1 text-sm font-medium focus:outline-none transition-all duration-200";
            sect.classList.remove('hidden');
        } else {
            btn.className = "border-transparent text-gray-400 hover:text-gray-200 hover:border-gray-600 border-b-2 py-4 px-1 text-sm font-medium focus:outline-none transition-all duration-200";
            sect.classList.add('hidden');
        }
    });
}

// Fetch Full State from API
async function fetchState() {
    try {
        const res = await fetch('/api/state');
        if (!res.ok) throw new Error('API request failed');
        const state = await res.json();
        lastState = state;
        updateDashboard(state);
    } catch (err) {
        console.error('Error fetching coordinator state:', err);
    }
}

// Trigger state polling every 1 second
setInterval(fetchState, 1000);
fetchState(); // run once immediately

// Update Dashboard UI Elements
function updateDashboard(state) {
    // 1. STATS
    const activeWorkersCount = state.workers.filter(w => w.active).length;
    document.getElementById('stat-active-workers').textContent = activeWorkersCount;
    document.getElementById('stat-total-jobs').textContent = state.jobs.length;
    document.getElementById('stat-total-tasks').textContent = state.map_tasks.length + state.reduce_tasks.length;

    // 2. TOPOLOGY GRAPH
    renderTopology(state);

    // 3. WORKERS LIST TABLE
    renderWorkers(state.workers);

    // 4. JOBS HISTORY LIST
    renderJobsHistory(state.jobs);

    // 5. SELECTED JOB VIEW PIPELINE
    if (selectedJobID === null && state.jobs.length > 0) {
        // Auto-select latest job
        selectedJobID = state.jobs[state.jobs.length - 1].id;
    }
    renderSelectedJobPipeline(state);

    // 6. DFS FILE TREE
    renderDfsExplorer(state.storage);
}

// Render Topology Graph (Tab 1)
function renderTopology(state) {
    const graphContainer = document.getElementById('topology-graph');
    if (!graphContainer) return;

    const activeWorkers = state.workers;
    
    let html = `
        <!-- Coordinator Center Node -->
        <div class="absolute top-1/2 left-1/2 transform -translate-x-1/2 -translate-y-1/2 z-10 flex flex-col items-center">
            <div class="w-16 h-16 bg-indigo-600 rounded-full border-4 border-indigo-400 flex items-center justify-center text-white font-bold text-xl shadow-xl hover:scale-105 transition duration-300 glow-active">
                COORD
            </div>
            <span class="mt-2 text-xs font-bold text-white bg-indigo-950 px-2.5 py-1 rounded-full border border-indigo-700">Coordinator RPC</span>
        </div>
    `;

    // Render workers positioned radially
    if (activeWorkers.length === 0) {
        html += `<div class="text-gray-500 text-sm mt-32">Waiting for worker nodes to register...</div>`;
    } else {
        const radius = 110; // radial distance from coordinator
        activeWorkers.forEach((w, index) => {
            const angle = (index * 2 * Math.PI) / activeWorkers.length;
            const x = Math.cos(angle) * radius;
            const y = Math.sin(angle) * radius;

            // Health color
            const colorClass = w.active ? 'bg-emerald-500 border-emerald-400' : 'bg-rose-500 border-rose-400';
            const statusLabel = w.active ? 'ACTIVE' : 'OFFLINE';

            // Random load metrics to simulate activity
            const simulatedCpu = w.active ? (Math.floor(Math.sin(Date.now() / 1000 + index) * 15) + 25) + '%' : '0%';

            html += `
                <!-- Connection Vector Line (SVG path) -->
                <svg class="absolute inset-0 w-full h-full pointer-events-none opacity-40">
                    <line x1="50%" y1="50%" x2="calc(50% + ${x}px)" y2="calc(50% + ${y}px)" 
                          stroke="${w.active ? '#10b981' : '#f43f5e'}" 
                          stroke-width="1.5" 
                          stroke-dasharray="${w.active ? '4, 4' : 'none'}" 
                          class="${w.active ? 'animate-[dash_10s_linear_infinite]' : ''}"/>
                </svg>

                <!-- Worker Radial Node -->
                <div style="transform: translate(calc(-50% + ${x}px), calc(-50% + ${y}px))"
                     class="absolute top-1/2 left-1/2 flex flex-col items-center group cursor-pointer transition-all duration-300 hover:scale-110">
                    <div class="w-10 h-10 rounded-full ${colorClass} border-2 flex items-center justify-center text-white font-bold text-xs shadow-md">
                        W${index + 1}
                    </div>
                    <div class="hidden group-hover:block absolute top-12 bg-gray-950 border border-gray-700 text-[10px] p-2 rounded-lg shadow-xl z-20 w-36 pointer-events-none">
                        <p class="font-bold text-white mb-0.5">${w.id}</p>
                        <p class="text-gray-400">Status: <span class="${w.active ? 'text-emerald-400' : 'text-rose-400'} font-bold">${statusLabel}</span></p>
                        <p class="text-gray-400">Load: <span class="text-indigo-400">${simulatedCpu} CPU</span></p>
                        <p class="text-gray-400">Processed: <span class="text-indigo-400">${w.tasks_handled} tasks</span></p>
                    </div>
                </div>
            `;
        });
    }

    graphContainer.innerHTML = html;
}

// Render Workers Table (Tab 1)
function renderWorkers(workers) {
    const tableBody = document.getElementById('worker-list-table');
    if (!tableBody) return;

    if (workers.length === 0) {
        tableBody.innerHTML = `
            <tr>
                <td colspan="6" class="px-6 py-8 text-center text-gray-500 text-sm">
                    No workers currently registered with the cluster.
                </td>
            </tr>
        `;
        return;
    }

    tableBody.innerHTML = workers.map((w, index) => {
        const isOnline = w.active;
        const statusBadge = isOnline 
            ? `<span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-semibold bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">Active</span>`
            : `<span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-semibold bg-rose-500/10 text-rose-400 border border-rose-500/20">Dead</span>`;

        const cpu = isOnline ? (Math.floor(Math.sin(Date.now() / 1500 + index) * 12) + 20) : 0;
        const mem = isOnline ? (Math.floor(Math.cos(Date.now() / 1500 + index) * 5) + 14) : 0;
        const healthBar = `
            <div class="flex items-center space-x-3 text-xs font-mono">
                <span class="text-gray-400">C: ${cpu}%</span>
                <div class="w-12 bg-gray-700 h-1.5 rounded-full overflow-hidden">
                    <div class="bg-indigo-500 h-full rounded-full" style="width: ${cpu}%"></div>
                </div>
                <span class="text-gray-400">M: ${mem}MB</span>
            </div>
        `;

        const lastSeenStr = isOnline 
            ? "Just now" 
            : new Date(w.last_seen).toLocaleTimeString();

        const killButton = isOnline 
            ? `<button onclick="killWorker('${w.id}')" class="text-rose-400 hover:text-rose-300 font-bold text-xs bg-rose-500/10 border border-rose-500/25 rounded px-2.5 py-1 transition duration-200">Simulate Crash</button>`
            : `<span class="text-gray-500 text-xs italic">Crashed</span>`;

        return `
            <tr class="hover:bg-gray-700/20 transition duration-150">
                <td class="px-6 py-4 font-mono text-sm text-indigo-300 font-bold">${w.id}</td>
                <td class="px-6 py-4">${statusBadge}</td>
                <td class="px-6 py-4">${healthBar}</td>
                <td class="px-6 py-4 font-mono text-sm text-gray-200">${w.tasks_handled}</td>
                <td class="px-6 py-4 font-mono text-xs text-gray-400">${lastSeenStr}</td>
                <td class="px-6 py-4 text-right">${killButton}</td>
            </tr>
        `;
    }).join('');
}

// Render Jobs History (Tab 2)
function renderJobsHistory(jobs) {
    const tableBody = document.getElementById('job-history-table');
    if (!tableBody) return;

    if (jobs.length === 0) {
        tableBody.innerHTML = `
            <tr>
                <td colspan="7" class="px-6 py-8 text-center text-gray-500 text-sm">
                    No jobs submitted yet. Use the submit form on the left to launch a job pipeline.
                </td>
            </tr>
        `;
        return;
    }

    tableBody.innerHTML = jobs.map(j => {
        const isSelected = j.id === selectedJobID;
        const rowClass = isSelected 
            ? 'bg-indigo-600/10 hover:bg-indigo-600/15 border-l-4 border-indigo-500' 
            : 'hover:bg-gray-700/20 border-l-4 border-transparent';

        const statusColors = {
            'PENDING': 'bg-gray-500/10 text-gray-400 border border-gray-500/20',
            'MAPPING': 'bg-blue-500/10 text-blue-400 border border-blue-500/20 animate-pulse',
            'SHUFFLING': 'bg-purple-500/10 text-purple-400 border border-purple-500/20 animate-pulse',
            'REDUCING': 'bg-yellow-500/10 text-yellow-400 border border-yellow-500/20 animate-pulse',
            'COMPLETED': 'bg-emerald-500/10 text-emerald-400 border border-emerald-500/20',
            'FAILED': 'bg-rose-500/10 text-rose-400 border border-rose-500/20'
        };

        const badge = `<span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-semibold ${statusColors[j.state]}">${j.state}</span>`;
        const start = new Date(j.created_at).toLocaleTimeString();
        const end = j.completed_at ? new Date(j.completed_at).toLocaleTimeString() : '-';

        return `
            <tr class="${rowClass} cursor-pointer transition duration-150" onclick="selectJob('${j.id}')">
                <td class="px-6 py-4 font-mono text-sm text-indigo-300 font-bold">${j.id}</td>
                <td class="px-6 py-4 text-xs font-semibold tracking-wider text-gray-300">${j.mapper.split('Mapper')[0]}</td>
                <td class="px-6 py-4">${badge}</td>
                <td class="px-6 py-4 font-mono text-xs text-gray-400">${j.num_map_tasks} Maps / ${j.num_reduce_tasks} Reduces</td>
                <td class="px-6 py-4 font-mono text-xs text-gray-400">${start}</td>
                <td class="px-6 py-4 font-mono text-xs text-gray-400">${end}</td>
                <td class="px-6 py-4 text-right">
                    <button class="text-indigo-400 hover:text-indigo-300 text-xs font-bold font-mono">Inspect &rarr;</button>
                </td>
            </tr>
        `;
    }).reverse().join('');
}

// Select a job to view in pipeline details
function selectJob(jobID) {
    selectedJobID = jobID;
    if (lastState) {
        renderSelectedJobPipeline(lastState);
    }
    // Scroll smoothly to the top of the page so the user sees the pipeline grid!
    window.scrollTo({ top: 0, behavior: 'smooth' });
}

// Render Selected Job Pipeline View (Tab 2)
function renderSelectedJobPipeline(state) {
    const jobTitle = document.getElementById('selected-job-id');
    const jobMeta = document.getElementById('selected-job-meta');
    const badge = document.getElementById('selected-job-badge');
    const mapTasksGrid = document.getElementById('grid-map-tasks');
    const reduceTasksGrid = document.getElementById('grid-reduce-tasks');

    if (!jobTitle || !mapTasksGrid || !reduceTasksGrid) return;

    if (!selectedJobID || state.jobs.length === 0) {
        jobTitle.textContent = "No job selected";
        jobMeta.textContent = "Submit a job to observe the dynamic MapReduce data processing steps.";
        badge.classList.add('hidden');
        mapTasksGrid.innerHTML = `<div class="text-gray-500 text-xs">Waiting for map tasks...</div>`;
        reduceTasksGrid.innerHTML = `<div class="text-gray-500 text-xs">Waiting for reduce tasks...</div>`;
        return;
    }

    const job = state.jobs.find(j => j.id === selectedJobID);
    if (!job) {
        selectedJobID = null;
        return;
    }

    // Set Header
    jobTitle.textContent = `Job Pipeline: ${job.ID}`;
    jobMeta.textContent = `Input: ${job.input_path}  |  Output: ${job.output_path}`;
    
    const statusColors = {
        'PENDING': 'bg-gray-500/10 text-gray-400 border border-gray-500/20',
        'MAPPING': 'bg-blue-500/10 text-blue-400 border border-blue-500/20',
        'SHUFFLING': 'bg-purple-500/10 text-purple-400 border border-purple-500/20',
        'REDUCING': 'bg-yellow-500/10 text-yellow-400 border border-yellow-500/20',
        'COMPLETED': 'bg-emerald-500/10 text-emerald-400 border border-emerald-500/20',
        'FAILED': 'bg-rose-500/10 text-rose-400 border border-rose-500/20'
    };
    badge.className = `px-2.5 py-1 rounded text-xs font-bold tracking-wider uppercase ${statusColors[job.state]}`;
    badge.textContent = job.state;
    badge.classList.remove('hidden');

    // Filter Tasks for this job
    const jobMaps = state.map_tasks.filter(t => t.job_id === job.id);
    const jobReduces = state.reduce_tasks.filter(t => t.job_id === job.id);

    // Calculate progress percents
    const completedMaps = jobMaps.filter(t => t.state === 'COMPLETED').length;
    const mapProgressPercent = jobMaps.length > 0 ? Math.round((completedMaps / jobMaps.length) * 100) : 0;

    const completedReduces = jobReduces.filter(t => t.state === 'COMPLETED').length;
    const reduceProgressPercent = jobReduces.length > 0 ? Math.round((completedReduces / jobReduces.length) * 100) : 0;

    // Handle Shuffling state simulation
    let shuffleProgressPercent = 0;
    if (job.state === 'SHUFFLING') {
        shuffleProgressPercent = 50;
    } else if (job.state === 'REDUCING' || job.state === 'COMPLETED') {
        shuffleProgressPercent = 100;
    }

    // Update Progress Indicators in cards
    document.getElementById('phase-prog-map').textContent = `${mapProgressPercent}%`;
    document.getElementById('phase-prog-shuffle').textContent = `${shuffleProgressPercent}%`;
    document.getElementById('phase-prog-reduce').textContent = `${reduceProgressPercent}%`;

    // Visual card highlighters
    ['map', 'shuffle', 'reduce'].forEach(p => {
        const card = document.getElementById(`phase-card-${p}`);
        card.className = "text-center p-3 rounded-lg border transition duration-300 bg-gray-900/30 border-transparent";
    });
    if (job.state === 'MAPPING') {
        document.getElementById('phase-card-map').className = "text-center p-3 rounded-lg border bg-blue-900/10 border-blue-500/30 animate-pulse";
    } else if (job.state === 'SHUFFLING') {
        document.getElementById('phase-card-shuffle').className = "text-center p-3 rounded-lg border bg-purple-900/10 border-purple-500/30 animate-pulse";
    } else if (job.state === 'REDUCING') {
        document.getElementById('phase-card-reduce').className = "text-center p-3 rounded-lg border bg-yellow-900/10 border-yellow-500/30 animate-pulse";
    }

    // Render task grids
    const taskStateColors = {
        'PENDING': 'bg-gray-700 hover:bg-gray-600 border border-gray-600',
        'ASSIGNED': 'bg-blue-600/40 hover:bg-blue-600/50 border border-blue-500',
        'RUNNING': 'bg-yellow-500/40 hover:bg-yellow-500/50 border border-yellow-400 animate-pulse',
        'COMPLETED': 'bg-emerald-600 hover:bg-emerald-500 border border-emerald-500',
        'FAILED': 'bg-rose-600 hover:bg-rose-500 border border-rose-500'
    };

    mapTasksGrid.innerHTML = jobMaps.map(t => {
        return `
            <button onclick="openTaskModal('${t.id}')"
                    title="${t.id} - ${t.state}"
                    class="task-square rounded font-bold text-[10px] flex items-center justify-center text-white ${taskStateColors[t.state]}">
                M${t.task_index}
            </button>
        `;
    }).join('');

    reduceTasksGrid.innerHTML = jobReduces.map(t => {
        return `
            <button onclick="openTaskModal('${t.id}')"
                    title="${t.id} - ${t.state}"
                    class="task-square rounded font-bold text-[10px] flex items-center justify-center text-white ${taskStateColors[t.state]}">
                R${t.task_index}
            </button>
        `;
    }).join('');
}

// Open details about a task (Modal)
function openTaskModal(taskID) {
    if (!lastState) return;

    let task = lastState.map_tasks.find(t => t.id === taskID);
    if (!task) {
        task = lastState.reduce_tasks.find(t => t.id === taskID);
    }
    if (!task) return;

    const modal = document.getElementById('task-modal');
    const title = document.getElementById('modal-task-title');
    const body = document.getElementById('modal-task-body');

    title.textContent = `Task Info: ${task.id}`;
    
    const assigned = new Date(task.assigned_at).toLocaleTimeString();
    const started = task.started_at ? new Date(task.started_at).toLocaleTimeString() : 'Pending';
    const completed = task.completed_at ? new Date(task.completed_at).toLocaleTimeString() : 'Running';

    body.innerHTML = `
        <div class="space-y-2.5 font-mono text-xs">
            <div class="flex justify-between border-b border-gray-700 pb-1.5">
                <span class="text-gray-400">Job ID:</span>
                <span class="text-indigo-400 font-bold">${task.job_id}</span>
            </div>
            <div class="flex justify-between border-b border-gray-700 pb-1.5">
                <span class="text-gray-400">Task Type:</span>
                <span class="text-gray-200">${task.task_type}</span>
            </div>
            <div class="flex justify-between border-b border-gray-700 pb-1.5">
                <span class="text-gray-400">Task Index:</span>
                <span class="text-gray-200">${task.task_index}</span>
            </div>
            <div class="flex justify-between border-b border-gray-700 pb-1.5">
                <span class="text-gray-400">Status State:</span>
                <span class="font-bold text-gray-200">${task.state}</span>
            </div>
            <div class="flex justify-between border-b border-gray-700 pb-1.5">
                <span class="text-gray-400">Assigned Worker:</span>
                <span class="text-indigo-300 font-bold">${task.worker_id || 'unassigned'}</span>
            </div>
            <div class="flex justify-between border-b border-gray-700 pb-1.5">
                <span class="text-gray-400">Dispatched:</span>
                <span class="text-gray-300">${assigned}</span>
            </div>
            <div class="flex justify-between border-b border-gray-700 pb-1.5">
                <span class="text-gray-400">Started Running:</span>
                <span class="text-gray-300">${started}</span>
            </div>
            <div class="flex justify-between border-b border-gray-700 pb-1.5">
                <span class="text-gray-400">Execution Ended:</span>
                <span class="text-gray-300">${completed}</span>
            </div>
            ${task.error ? `
            <div class="bg-rose-500/10 border border-rose-500/25 rounded p-3 text-rose-400 mt-2 whitespace-pre-wrap">
                <p class="font-bold text-xs uppercase tracking-wider mb-1">Execution Error:</p>
                ${task.error}
            </div>
            ` : ''}
        </div>
    `;

    modal.classList.remove('hidden');
    setTimeout(() => {
        modal.classList.remove('opacity-0');
        modal.firstElementChild.classList.remove('scale-95');
    }, 10);
}

function closeTaskModal() {
    const modal = document.getElementById('task-modal');
    modal.classList.add('opacity-0');
    modal.firstElementChild.classList.add('scale-95');
    setTimeout(() => {
        modal.classList.add('hidden');
    }, 300);
}

// Render DFS Explorer Folder Tree (Tab 3)
function renderDfsExplorer(files) {
    const tree = document.getElementById('dfs-file-tree');
    if (!tree) return;

    if (files.length === 0) {
        tree.innerHTML = `<div class="text-gray-500 text-xs italic">Storage empty. Use hdfs file writer above.</div>`;
        return;
    }

    // Group files into structural subdirectories
    // This allows us to display a beautiful nested explorer folder tree!
    let html = '';
    
    // Virtual structural root maps
    const groups = {
        'input/': [],
        'intermediate/': [],
        'output/': [],
        'other': []
    };

    files.forEach(f => {
        if (f.path.startsWith('input/')) {
            groups['input/'].push(f);
        } else if (f.path.startsWith('intermediate/')) {
            groups['intermediate/'].push(f);
        } else if (f.path.startsWith('output/')) {
            groups['output/'].push(f);
        } else {
            groups['other'].push(f);
        }
    });

    Object.entries(groups).forEach(([folderName, items]) => {
        if (folderName !== 'other') {
            html += `
                <div class="mb-2">
                    <div class="flex items-center space-x-1.5 text-indigo-400 font-bold font-sans cursor-default select-none">
                        <span>📁</span>
                        <span>${folderName}</span>
                        <span class="text-[10px] text-gray-500 font-normal">(${items.length} items)</span>
                    </div>
                    <div class="pl-4 border-l border-gray-800 ml-2 mt-1 space-y-1">
            `;
            if (items.length === 0) {
                html += `<div class="text-gray-600 text-[11px] italic pl-2">Empty</div>`;
            } else {
                items.forEach(item => {
                    if (item.is_dir) return; // skip subfolders in lists, render flat files inside parent
                    html += renderFileItem(item);
                });
            }
            html += `</div></div>`;
        } else if (items.length > 0) {
            items.forEach(item => {
                if (item.is_dir) return;
                html += renderFileItem(item);
            });
        }
    });

    tree.innerHTML = html;
}

function renderFileItem(item) {
    return `
        <div onclick="previewFile('${item.path}')" 
             class="flex justify-between items-center px-2 py-1 rounded hover:bg-gray-800/40 cursor-pointer group text-xs text-gray-300 font-mono">
            <span class="truncate group-hover:text-indigo-400">📄 ${item.name}</span>
            <span class="text-[10px] text-gray-500 font-sans">${formatBytes(item.size)}</span>
        </div>
    `;
}

// Format bytes size
function formatBytes(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

// Preview File (DFS Inspector)
async function previewFile(path) {
    const title = document.getElementById('dfs-preview-title');
    const meta = document.getElementById('dfs-preview-meta');
    const sizeEl = document.getElementById('dfs-preview-size');
    const body = document.getElementById('dfs-preview-body');

    title.textContent = "Loading file content...";
    body.textContent = "Loading...";

    try {
        const res = await fetch(`/api/file/content?path=${encodeURIComponent(path)}`);
        if (!res.ok) throw new Error('Failed to read file contents');
        const text = await res.text();

        title.textContent = path.split('/').pop();
        meta.textContent = `HDFS Path: /${path}`;
        
        // Find size from last state
        const fileObj = lastState.storage.find(f => f.path === path);
        sizeEl.textContent = fileObj ? formatBytes(fileObj.size) : 'Unknown size';

        body.textContent = text;
    } catch (err) {
        title.textContent = "Error Loading File";
        body.textContent = "Could not preview file contents: " + err.message;
    }
}

// Write custom input file to DFS
async function writeCustomFile() {
    const filenameInput = document.getElementById('dfs-filename');
    const contentInput = document.getElementById('dfs-content');

    const filename = filenameInput.value.trim();
    const content = contentInput.value.trim();

    if (!filename || !content) {
        alert('Please specify both a filename (e.g. text.txt) and write content!');
        return;
    }

    try {
        const res = await fetch('/api/file/write', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ filename, content })
        });
        if (!res.ok) throw new Error(await res.text());
        
        // Success
        filenameInput.value = '';
        contentInput.value = '';
        fetchState(); // refresh
        alert('File successfully saved to ./shared-hdfs/input/' + filename + '. You can now run a job on it!');
    } catch (err) {
        alert('Failed to save file: ' + err.message);
    }
}

// Submit a new job via HTTP API
async function submitNewJob(e) {
    e.preventDefault();

    const jobType = document.getElementById('form-job-type').value;
    const mappers = parseInt(document.getElementById('form-mappers').value);
    const reducers = parseInt(document.getElementById('form-reducers').value);

    try {
        const res = await fetch('/api/submit', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                job_type: jobType,
                num_map_tasks: mappers,
                num_reduce_tasks: reducers
            })
        });

        if (!res.ok) throw new Error(await res.text());
        const result = await res.json();
        
        if (result.success) {
            selectedJobID = result.job_id;
            switchTab('pipeline'); // switch to see progress bar immediately
            fetchState();
        } else {
            alert('Failed to submit job: ' + result.error);
        }
    } catch (err) {
        alert('Submit job error: ' + err.message);
    }
}

// Chaos Testing: kill a worker
async function killWorker(workerID) {
    if (!confirm(`Are you sure you want to simulate a crash on worker ${workerID}?`)) return;

    try {
        const res = await fetch('/api/workers/kill', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ worker_id: workerID })
        });
        if (!res.ok) throw new Error('API failure');
        
        fetchState();
    } catch (err) {
        alert('Chaos call failed: ' + err.message);
    }
}
