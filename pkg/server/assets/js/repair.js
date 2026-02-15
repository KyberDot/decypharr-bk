// Repair management for Decypharr (v2-aware)
class RepairManager {
    constructor() {
        this.state = {
            jobs: [],
            currentJob: null,
            allBrokenItems: [],
            filteredItems: [],
            currentPage: 1,
            currentItemsPage: 1,
            itemsPerPage: 10,
            itemsPerModalPage: 20,
            sortBy: 'created_at',
            sortDirection: 'desc'
        };

        this.refs = {
            repairForm: document.getElementById('repairForm'),
            repairScope: document.getElementById('repairScope'),
            repairScopeDescription: document.getElementById('repairScopeDescription'),
            arrSelect: document.getElementById('arrSelect'),
            arrHelpText: document.getElementById('arrHelpText'),
            mediaIds: document.getElementById('mediaIds'),
            mediaHelpText: document.getElementById('mediaHelpText'),
            repairMode: document.getElementById('repairMode'),
            repairModeDescription: document.getElementById('repairModeDescription'),
            submitBtn: document.getElementById('submitRepair'),

            jobsTableBody: document.getElementById('jobsTableBody'),
            jobsPagination: document.getElementById('jobsPagination'),
            noJobsMessage: document.getElementById('noJobsMessage'),
            refreshJobs: document.getElementById('refreshJobs'),
            deleteSelectedJobs: document.getElementById('deleteSelectedJobs'),
            selectAllJobs: document.getElementById('selectAllJobs'),

            jobDetailsModal: document.getElementById('jobDetailsModal'),
            modalJobId: document.getElementById('modalJobId'),
            modalJobStatus: document.getElementById('modalJobStatus'),
            modalJobStage: document.getElementById('modalJobStage'),
            modalJobStarted: document.getElementById('modalJobStarted'),
            modalJobCompleted: document.getElementById('modalJobCompleted'),
            modalJobArrs: document.getElementById('modalJobArrs'),
            modalJobMediaIds: document.getElementById('modalJobMediaIds'),
            modalJobMode: document.getElementById('modalJobMode'),
            modalJobAutoProcess: document.getElementById('modalJobAutoProcess'),
            modalJobError: document.getElementById('modalJobError'),
            errorContainer: document.getElementById('errorContainer'),

            modalStatDiscovered: document.getElementById('modalStatDiscovered'),
            modalStatProbed: document.getElementById('modalStatProbed'),
            modalStatBroken: document.getElementById('modalStatBroken'),
            modalStatUnknown: document.getElementById('modalStatUnknown'),
            modalStatPlanned: document.getElementById('modalStatPlanned'),
            modalStatExecuted: document.getElementById('modalStatExecuted'),
            modalStatFixed: document.getElementById('modalStatFixed'),
            modalStatFailed: document.getElementById('modalStatFailed'),

            actionItemsTableBody: document.getElementById('actionItemsTableBody'),
            noActionsMessage: document.getElementById('noActionsMessage'),

            brokenItemsTableBody: document.getElementById('brokenItemsTableBody'),
            itemsPagination: document.getElementById('itemsPagination'),
            noBrokenItemsMessage: document.getElementById('noBrokenItemsMessage'),
            noFilteredItemsMessage: document.getElementById('noFilteredItemsMessage'),
            totalItemsCount: document.getElementById('totalItemsCount'),
            modalFooterStats: document.getElementById('modalFooterStats'),

            itemSearchInput: document.getElementById('itemSearchInput'),
            arrFilterSelect: document.getElementById('arrFilterSelect'),
            pathFilterSelect: document.getElementById('pathFilterSelect'),
            clearFiltersBtn: document.getElementById('clearFiltersBtn'),

            processJobBtn: document.getElementById('processJobBtn'),
            stopJobBtn: document.getElementById('stopJobBtn')
        };

        this.init();
    }

    init() {
        this.bindEvents();
        this.syncScopeUI();
        this.syncModeUI();
        this.loadArrInstances();
        this.loadJobs();
        this.startAutoRefresh();
    }

    bindEvents() {
        this.refs.repairForm?.addEventListener('submit', (e) => this.handleFormSubmit(e));
        this.refs.refreshJobs?.addEventListener('click', () => this.loadJobs());
        this.refs.deleteSelectedJobs?.addEventListener('click', () => this.deleteSelectedJobs());
        this.refs.selectAllJobs?.addEventListener('change', (e) => this.toggleSelectAllJobs(e.target.checked));

        this.refs.repairMode?.addEventListener('change', () => this.syncModeUI());
        this.refs.repairScope?.addEventListener('change', () => {
            this.syncScopeUI();
            this.syncModeUI();
        });

        this.refs.processJobBtn?.addEventListener('click', () => this.processCurrentJob());
        this.refs.stopJobBtn?.addEventListener('click', () => this.stopCurrentJob());

        this.refs.itemSearchInput?.addEventListener('input', window.decypharrUtils.debounce(() => this.applyFilters(), 300));
        this.refs.arrFilterSelect?.addEventListener('change', () => this.applyFilters());
        this.refs.pathFilterSelect?.addEventListener('change', () => this.applyFilters());
        this.refs.clearFiltersBtn?.addEventListener('click', () => this.clearFilters());

        this.refs.jobsTableBody?.addEventListener('click', (e) => this.handleJobTableClick(e));
        this.refs.brokenItemsTableBody?.addEventListener('click', (e) => this.handleItemTableClick(e));
    }

    getSelectedScope() {
        const scope = this.refs.repairScope?.value;
        return scope === 'managed_entries' ? 'managed_entries' : 'arr';
    }

    isManagedEntriesScope() {
        return this.getSelectedScope() === 'managed_entries';
    }

    syncScopeUI() {
        const managedEntriesScope = this.isManagedEntriesScope();

        if (this.refs.arrSelect) {
            this.refs.arrSelect.disabled = managedEntriesScope;
            if (managedEntriesScope) {
                this.refs.arrSelect.value = '';
            }
        }
        if (this.refs.arrHelpText) {
            this.refs.arrHelpText.textContent = managedEntriesScope
                ? 'Arr selection is disabled for managed-entry scans.'
                : 'Choose which Arr service to repair';
        }
        if (this.refs.mediaIds) {
            this.refs.mediaIds.placeholder = managedEntriesScope ? 'optional: infohash, entry name' : '123, 456, 789';
        }
        if (this.refs.mediaHelpText) {
            this.refs.mediaHelpText.textContent = managedEntriesScope
                ? 'Optional managed-entry filters (infohash or entry name), comma-separated. Leave empty for all entries.'
                : 'Enter specific TV DB IDs (Sonarr) or TM DB IDs (Radarr), comma-separated. Leave empty for all media.';
        }
        if (this.refs.repairScopeDescription) {
            this.refs.repairScopeDescription.textContent = managedEntriesScope
                ? 'Managed-entry scope probes Decypharr tracked entries directly, without Arr metadata.'
                : 'Arr media scope discovers files through Arr-managed library items.';
        }
    }

    syncModeUI() {
        if (!this.refs.repairModeDescription || !this.refs.submitBtn) return;
        const mode = this.refs.repairMode.value;
        const managedEntriesScope = this.isManagedEntriesScope();

        if (mode === 'detect_and_repair') {
            this.refs.repairModeDescription.textContent =
                managedEntriesScope
                    ? 'Runs detect + repair on managed entries (reinsert actions where available), then verifies.'
                    : 'Runs full workflow: detect broken files, execute repair actions, then verify.';
            this.refs.submitBtn.innerHTML = '<i class="bi bi-wrench-adjustable mr-2"></i>Start Detect + Repair';
            return;
        }

        this.refs.repairModeDescription.textContent =
            'Detect-only scans and reports broken files without changing anything.';
        this.refs.submitBtn.innerHTML = '<i class="bi bi-search mr-2"></i>Start Detect Only';
    }

    async loadArrInstances() {
        try {
            const response = await window.decypharrUtils.fetcher('/api/arrs');
            if (!response.ok) throw new Error('Failed to load Arr instances');

            const arrs = await response.json();
            this.refs.arrSelect.innerHTML = '<option value="">Select an Arr instance</option>';

            arrs.forEach((arr) => {
                const option = document.createElement('option');
                option.value = arr.name;
                option.textContent = `${arr.name} (${arr.host})`;
                this.refs.arrSelect.appendChild(option);
            });
        } catch (error) {
            console.error('Error loading Arr instances:', error);
            window.decypharrUtils.createToast('Failed to load Arr instances', 'error');
        }
    }

    async handleFormSubmit(e) {
        e.preventDefault();

        const scope = this.getSelectedScope();
        const arr = this.refs.arrSelect.value;
        const mediaIdsValue = this.refs.mediaIds.value.trim();
        const mode = this.refs.repairMode.value || 'detect_only';

        const mediaIds = mediaIdsValue
            ? mediaIdsValue.split(',').map((id) => id.trim()).filter(Boolean)
            : [];

        const requestBody = {
            arr,
            mediaIds: mediaIds.length > 0 ? mediaIds : null,
            scope,
            mode,
            autoProcess: mode === 'detect_and_repair'
        };

        try {
            window.decypharrUtils.setButtonLoading(this.refs.submitBtn, true);

            const response = await window.decypharrUtils.fetcher('/api/repair', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(requestBody)
            });

            if (!response.ok) {
                const errorText = await response.text();
                throw new Error(errorText || 'Failed to start repair');
            }

            const result = await response.json();
            window.decypharrUtils.createToast(
                `Repair run started. ID: ${result.job_id?.substring(0, 8) || 'unknown'}`,
                'success'
            );

            this.refs.mediaIds.value = '';
            await this.loadJobs();
        } catch (error) {
            console.error('Error starting repair:', error);
            window.decypharrUtils.createToast(`Error starting repair: ${error.message}`, 'error');
        } finally {
            window.decypharrUtils.setButtonLoading(this.refs.submitBtn, false);
        }
    }

    async loadJobs() {
        try {
            const response = await window.decypharrUtils.fetcher('/api/repair/jobs');
            if (!response.ok) throw new Error('Failed to fetch jobs');

            const data = await response.json();
            this.state.jobs = Array.isArray(data) ? data : [];
            this.renderJobsTable();
        } catch (error) {
            console.error('Error loading jobs:', error);
            window.decypharrUtils.createToast('Error loading repair jobs', 'error');
        }
    }

    extractStats(job) {
        const zero = {
            discovered: 0,
            probed: 0,
            broken: 0,
            planned: 0,
            executed: 0,
            fixed: 0,
            failed: 0,
            unknown: 0
        };

        const stats = { ...zero, ...(job?.stats || {}) };
        if (stats.broken === 0 && job?.broken_items) {
            stats.broken = Object.values(job.broken_items).reduce((sum, items) => sum + (items?.length || 0), 0);
        }
        return stats;
    }

    getJobStatus(status) {
        const map = {
            pending: { text: 'Pending', class: 'badge-warning' },
            started: { text: 'Running', class: 'badge-primary' },
            processing: { text: 'Executing', class: 'badge-info' },
            completed: { text: 'Completed', class: 'badge-success' },
            failed: { text: 'Failed', class: 'badge-error' },
            cancelled: { text: 'Cancelled', class: 'badge-ghost' }
        };
        return map[status] || { text: status || 'unknown', class: 'badge-ghost' };
    }

    getJobStage(stage) {
        const map = {
            queued: { text: 'Queued', class: 'badge-ghost' },
            discovering: { text: 'Discovering', class: 'badge-primary' },
            probing: { text: 'Probing', class: 'badge-primary' },
            planning: { text: 'Planning', class: 'badge-info' },
            executing: { text: 'Executing', class: 'badge-warning' },
            verifying: { text: 'Verifying', class: 'badge-warning' },
            completed: { text: 'Completed', class: 'badge-success' },
            failed: { text: 'Failed', class: 'badge-error' },
            cancelled: { text: 'Cancelled', class: 'badge-ghost' }
        };
        return map[stage] || { text: stage || '-', class: 'badge-ghost' };
    }

    formatMode(mode, autoProcess) {
        if (!mode) {
            return autoProcess ? 'Detect + Repair' : 'Detect Only';
        }
        return mode === 'detect_and_repair' ? 'Detect + Repair' : 'Detect Only';
    }

    getSortedJobs() {
        const jobs = [...this.state.jobs];

        jobs.sort((a, b) => {
            let valueA = '';
            let valueB = '';

            switch (this.state.sortBy) {
                case 'created_at':
                    valueA = new Date(a.created_at || a.started_at || 0).getTime();
                    valueB = new Date(b.created_at || b.started_at || 0).getTime();
                    break;
                case 'status':
                    valueA = a.status || '';
                    valueB = b.status || '';
                    break;
                default:
                    valueA = a[this.state.sortBy] || '';
                    valueB = b[this.state.sortBy] || '';
            }

            if (typeof valueA === 'string') {
                return this.state.sortDirection === 'asc'
                    ? valueA.localeCompare(valueB)
                    : valueB.localeCompare(valueA);
            }
            return this.state.sortDirection === 'asc' ? valueA - valueB : valueB - valueA;
        });

        return jobs;
    }

    renderJobsTable() {
        const jobs = this.getSortedJobs();
        const totalPages = Math.max(1, Math.ceil(jobs.length / this.state.itemsPerPage));
        if (this.state.currentPage > totalPages) {
            this.state.currentPage = totalPages;
        }

        const startIndex = (this.state.currentPage - 1) * this.state.itemsPerPage;
        const endIndex = Math.min(startIndex + this.state.itemsPerPage, jobs.length);
        const pageJobs = jobs.slice(startIndex, endIndex);

        this.refs.jobsTableBody.innerHTML = '';
        this.refs.jobsPagination.innerHTML = '';

        this.refs.selectAllJobs.checked = false;
        this.refs.deleteSelectedJobs.disabled = true;

        if (jobs.length === 0) {
            this.refs.noJobsMessage.classList.remove('hidden');
            return;
        }

        this.refs.noJobsMessage.classList.add('hidden');

        pageJobs.forEach((job) => {
            this.refs.jobsTableBody.appendChild(this.createJobRow(job));
        });

        this.renderJobsPagination(totalPages);
        this.updateJobSelectionState();
    }

    createJobRow(job) {
        const row = document.createElement('tr');
        row.className = 'hover:bg-base-200 transition-colors';
        row.dataset.jobId = job.id;

        const status = this.getJobStatus(job.status);
        const stage = this.getJobStage(job.stage);
        const modeText = this.formatMode(job.mode, job.auto_process);
        const startedDate = this.formatDate(job.created_at);
        const stats = this.extractStats(job);
        const canDelete = !['started', 'processing'].includes(job.status);

        const arrs = (job.arrs && job.arrs.length > 0) ? job.arrs : ['managed_entries'];
        row.innerHTML = `
            <td>
                <label class="cursor-pointer">
                    <input type="checkbox" class="checkbox checkbox-sm checkbox-primary job-checkbox"
                           value="${job.id}" ${canDelete ? '' : 'disabled'}>
                </label>
            </td>
            <td>
                <button class="btn btn-ghost btn-xs font-mono view-job" data-job-id="${job.id}">
                    ${window.decypharrUtils.escapeHtml(job.id.substring(0, 8))}...
                </button>
            </td>
            <td>
                <div class="flex flex-wrap gap-1">
                    ${arrs.map((arr) => `<div class="badge badge-secondary badge-xs">${window.decypharrUtils.escapeHtml(arr)}</div>`).join('')}
                </div>
            </td>
            <td>
                <time class="text-sm" datetime="${window.decypharrUtils.escapeHtml(job.created_at || '')}">${startedDate}</time>
            </td>
            <td><div class="badge ${status.class} badge-sm">${status.text}</div></td>
            <td><div class="badge ${stage.class} badge-sm">${stage.text}</div></td>
            <td><div class="badge badge-outline badge-sm">${modeText}</div></td>
            <td>
                <div class="text-xs font-mono leading-tight">
                    B:${stats.broken} F:${stats.fixed} U:${stats.unknown}
                </div>
            </td>
            <td>
                <div class="flex gap-1">
                    ${job.status === 'pending' ? `
                        <button class="btn btn-primary btn-xs process-job" data-job-id="${job.id}" title="Execute pending run">
                            <i class="bi bi-play-fill"></i>
                        </button>` : ''}
                    ${['started', 'processing'].includes(job.status) ? `
                        <button class="btn btn-warning btn-xs stop-job" data-job-id="${job.id}" title="Stop run">
                            <i class="bi bi-stop-fill"></i>
                        </button>` : ''}
                    ${canDelete ? `
                        <button class="btn btn-error btn-xs delete-job" data-job-id="${job.id}" title="Delete run">
                            <i class="bi bi-trash"></i>
                        </button>` : `
                        <button class="btn btn-error btn-xs" disabled><i class="bi bi-trash"></i></button>`}
                </div>
            </td>
        `;

        return row;
    }

    renderJobsPagination(totalPages) {
        if (totalPages <= 1) return;

        const pagination = document.createElement('div');
        pagination.className = 'join';

        const prevBtn = document.createElement('button');
        prevBtn.className = `join-item btn btn-sm ${this.state.currentPage === 1 ? 'btn-disabled' : ''}`;
        prevBtn.innerHTML = '<i class="bi bi-chevron-left"></i>';
        prevBtn.disabled = this.state.currentPage === 1;
        if (this.state.currentPage > 1) {
            prevBtn.addEventListener('click', () => {
                this.state.currentPage--;
                this.renderJobsTable();
            });
        }
        pagination.appendChild(prevBtn);

        const maxButtons = 5;
        let startPage = Math.max(1, this.state.currentPage - Math.floor(maxButtons / 2));
        let endPage = Math.min(totalPages, startPage + maxButtons - 1);
        if (endPage - startPage + 1 < maxButtons) {
            startPage = Math.max(1, endPage - maxButtons + 1);
        }

        for (let i = startPage; i <= endPage; i++) {
            const pageBtn = document.createElement('button');
            pageBtn.className = `join-item btn btn-sm ${i === this.state.currentPage ? 'btn-active' : ''}`;
            pageBtn.textContent = i;
            pageBtn.addEventListener('click', () => {
                this.state.currentPage = i;
                this.renderJobsTable();
            });
            pagination.appendChild(pageBtn);
        }

        const nextBtn = document.createElement('button');
        nextBtn.className = `join-item btn btn-sm ${this.state.currentPage === totalPages ? 'btn-disabled' : ''}`;
        nextBtn.innerHTML = '<i class="bi bi-chevron-right"></i>';
        nextBtn.disabled = this.state.currentPage === totalPages;
        if (this.state.currentPage < totalPages) {
            nextBtn.addEventListener('click', () => {
                this.state.currentPage++;
                this.renderJobsTable();
            });
        }
        pagination.appendChild(nextBtn);

        this.refs.jobsPagination.appendChild(pagination);
    }

    handleJobTableClick(e) {
        const checkbox = e.target.closest('.job-checkbox');
        if (checkbox) {
            this.updateJobSelectionState();
            return;
        }

        const target = e.target.closest('button');
        if (!target) return;

        const jobId = target.dataset.jobId;
        if (!jobId) return;

        if (target.classList.contains('view-job')) {
            this.viewJobDetails(jobId);
        } else if (target.classList.contains('process-job')) {
            this.processJob(jobId);
        } else if (target.classList.contains('stop-job')) {
            this.stopJob(jobId);
        } else if (target.classList.contains('delete-job')) {
            this.deleteJob(jobId);
        }
    }

    async viewJobDetails(jobId) {
        const job = this.state.jobs.find((j) => j.id === jobId);
        if (!job) return;

        this.state.currentJob = job;
        this.populateJobModal(job);
        this.refs.jobDetailsModal.showModal();
    }

    populateJobModal(job) {
        const status = this.getJobStatus(job.status);
        const stage = this.getJobStage(job.stage);
        const stats = this.extractStats(job);

        this.refs.modalJobId.textContent = job.id.substring(0, 8);
        this.refs.modalJobStatus.innerHTML = `<span class="badge ${status.class}">${status.text}</span>`;
        this.refs.modalJobStage.innerHTML = `<span class="badge ${stage.class}">${stage.text}</span>`;
        this.refs.modalJobStarted.textContent = this.formatDate(job.created_at);
        this.refs.modalJobCompleted.textContent = this.formatDate(job.finished_at);

        this.refs.modalJobArrs.textContent = (job.arrs && job.arrs.length > 0) ? job.arrs.join(', ') : 'managed_entries';
        this.refs.modalJobMediaIds.textContent = (job.media_ids && job.media_ids.length > 0) ? job.media_ids.join(', ') : 'All media';
        this.refs.modalJobMode.textContent = this.formatMode(job.mode, job.auto_process);
        this.refs.modalJobAutoProcess.textContent = job.auto_process ? 'Yes' : 'No';

        this.setStat(this.refs.modalStatDiscovered, stats.discovered);
        this.setStat(this.refs.modalStatProbed, stats.probed);
        this.setStat(this.refs.modalStatBroken, stats.broken);
        this.setStat(this.refs.modalStatUnknown, stats.unknown);
        this.setStat(this.refs.modalStatPlanned, stats.planned);
        this.setStat(this.refs.modalStatExecuted, stats.executed);
        this.setStat(this.refs.modalStatFixed, stats.fixed);
        this.setStat(this.refs.modalStatFailed, stats.failed);

        if (job.error) {
            this.refs.modalJobError.textContent = job.error;
            this.refs.errorContainer.classList.remove('hidden');
        } else {
            this.refs.errorContainer.classList.add('hidden');
        }

        this.refs.processJobBtn.classList.toggle('hidden', job.status !== 'pending');
        this.refs.stopJobBtn.classList.toggle('hidden', !['started', 'processing'].includes(job.status));

        this.renderActions(job.actions || []);

        if (job.broken_items) {
            this.state.allBrokenItems = this.processItemsData(job.broken_items);
            this.state.filteredItems = [...this.state.allBrokenItems];
        } else {
            this.state.allBrokenItems = [];
            this.state.filteredItems = [];
        }

        this.populateArrFilter();
        this.state.currentItemsPage = 1;
        this.renderBrokenItemsTable();
        this.updateItemsStats();
    }

    renderActions(actions) {
        this.refs.actionItemsTableBody.innerHTML = '';

        if (!actions || actions.length === 0) {
            this.refs.noActionsMessage.classList.remove('hidden');
            return;
        }

        this.refs.noActionsMessage.classList.add('hidden');

        actions.forEach((action) => {
            const row = document.createElement('tr');
            const status = this.getActionStatus(action.status);

            row.innerHTML = `
                <td class="font-mono text-xs">${window.decypharrUtils.escapeHtml(action.type || '-')}</td>
                <td>${window.decypharrUtils.escapeHtml(action.protocol || '-')}</td>
                <td class="font-mono text-xs">${window.decypharrUtils.escapeHtml((action.entry_id || '-').substring(0, 16))}</td>
                <td><span class="badge ${status.class} badge-xs">${status.text}</span></td>
                <td class="text-xs">${this.formatDate(action.started_at)}</td>
                <td class="text-xs">${this.formatDate(action.completed_at)}</td>
                <td class="text-xs text-error max-w-56 truncate" title="${window.decypharrUtils.escapeHtml(action.error || '')}">
                    ${window.decypharrUtils.escapeHtml(action.error || '-')}
                </td>
            `;
            this.refs.actionItemsTableBody.appendChild(row);
        });
    }

    getActionStatus(status) {
        const map = {
            planned: { text: 'Planned', class: 'badge-ghost' },
            running: { text: 'Running', class: 'badge-primary' },
            succeeded: { text: 'Succeeded', class: 'badge-success' },
            failed: { text: 'Failed', class: 'badge-error' },
            skipped: { text: 'Skipped', class: 'badge-warning' }
        };
        return map[status] || { text: status || '-', class: 'badge-ghost' };
    }

    setStat(ref, value) {
        if (ref) {
            ref.textContent = Number(value || 0).toString();
        }
    }

    processItemsData(brokenItems) {
        const items = [];

        Object.entries(brokenItems).forEach(([arrName, entries]) => {
            (entries || []).forEach((item, index) => {
                items.push({
                    id: `${arrName}-${index}`,
                    arr: arrName,
                    path: item.path || item.file_path || item.targetPath || 'Unknown path',
                    size: item.size || 0,
                    type: this.getFileType(item.path || item.targetPath || ''),
                    fileId: item.fileId || item.id || `${arrName}-${index}`
                });
            });
        });

        return items;
    }

    getFileType(path) {
        const movieExtensions = ['.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm'];
        const tvIndicators = ['/tv/', '/television/', '/series/', '/shows/'];

        const lower = path.toLowerCase();
        if (tvIndicators.some((indicator) => lower.includes(indicator))) return 'tv';
        if (movieExtensions.some((ext) => lower.endsWith(ext))) {
            return lower.includes('/movies/') || lower.includes('/films/') ? 'movie' : 'tv';
        }
        return 'other';
    }

    populateArrFilter() {
        this.refs.arrFilterSelect.innerHTML = '<option value="">All Arrs</option>';
        const uniqueArrs = [...new Set(this.state.allBrokenItems.map((item) => item.arr))];

        uniqueArrs.forEach((arr) => {
            const option = document.createElement('option');
            option.value = arr;
            option.textContent = arr;
            this.refs.arrFilterSelect.appendChild(option);
        });
    }

    applyFilters() {
        const searchTerm = this.refs.itemSearchInput.value.toLowerCase();
        const arrFilter = this.refs.arrFilterSelect.value;
        const pathFilter = this.refs.pathFilterSelect.value;

        this.state.filteredItems = this.state.allBrokenItems.filter((item) => {
            const matchesSearch = !searchTerm || item.path.toLowerCase().includes(searchTerm);
            const matchesArr = !arrFilter || item.arr === arrFilter;
            const matchesPath = !pathFilter || item.type === pathFilter;
            return matchesSearch && matchesArr && matchesPath;
        });

        this.state.currentItemsPage = 1;
        this.renderBrokenItemsTable();
        this.updateItemsStats();
    }

    clearFilters() {
        this.refs.itemSearchInput.value = '';
        this.refs.arrFilterSelect.value = '';
        this.refs.pathFilterSelect.value = '';
        this.applyFilters();
    }

    renderBrokenItemsTable() {
        this.refs.brokenItemsTableBody.innerHTML = '';
        this.refs.itemsPagination.innerHTML = '';

        if (this.state.allBrokenItems.length === 0) {
            this.refs.noBrokenItemsMessage.classList.remove('hidden');
            this.refs.noFilteredItemsMessage.classList.add('hidden');
            return;
        }

        if (this.state.filteredItems.length === 0) {
            this.refs.noBrokenItemsMessage.classList.add('hidden');
            this.refs.noFilteredItemsMessage.classList.remove('hidden');
            return;
        }

        this.refs.noBrokenItemsMessage.classList.add('hidden');
        this.refs.noFilteredItemsMessage.classList.add('hidden');

        const totalPages = Math.ceil(this.state.filteredItems.length / this.state.itemsPerModalPage);
        const startIndex = (this.state.currentItemsPage - 1) * this.state.itemsPerModalPage;
        const endIndex = Math.min(startIndex + this.state.itemsPerModalPage, this.state.filteredItems.length);
        const pageItems = this.state.filteredItems.slice(startIndex, endIndex);

        pageItems.forEach((item) => {
            this.refs.brokenItemsTableBody.appendChild(this.createBrokenItemRow(item));
        });

        this.renderItemsPagination(totalPages);
    }

    createBrokenItemRow(item) {
        const row = document.createElement('tr');
        row.className = 'hover:bg-base-200 transition-colors';

        const typeColor = {
            movie: 'badge-primary',
            tv: 'badge-secondary',
            other: 'badge-ghost'
        };

        row.innerHTML = `
            <td><div class="badge badge-info badge-xs">${window.decypharrUtils.escapeHtml(item.arr)}</div></td>
            <td>
                <div class="text-sm max-w-xs truncate" title="${window.decypharrUtils.escapeHtml(item.path)}">
                    ${window.decypharrUtils.escapeHtml(item.path)}
                </div>
            </td>
            <td><div class="badge ${typeColor[item.type]} badge-xs">${item.type}</div></td>
            <td><span class="text-sm font-mono">${window.decypharrUtils.formatBytes(item.size)}</span></td>
        `;

        return row;
    }

    renderItemsPagination(totalPages) {
        if (totalPages <= 1) return;

        const pagination = document.createElement('div');
        pagination.className = 'join';

        const prevBtn = document.createElement('button');
        prevBtn.className = `join-item btn btn-sm ${this.state.currentItemsPage === 1 ? 'btn-disabled' : ''}`;
        prevBtn.innerHTML = '<i class="bi bi-chevron-left"></i>';
        prevBtn.disabled = this.state.currentItemsPage === 1;
        if (this.state.currentItemsPage > 1) {
            prevBtn.addEventListener('click', () => {
                this.state.currentItemsPage--;
                this.renderBrokenItemsTable();
            });
        }
        pagination.appendChild(prevBtn);

        const maxButtons = 5;
        let startPage = Math.max(1, this.state.currentItemsPage - Math.floor(maxButtons / 2));
        let endPage = Math.min(totalPages, startPage + maxButtons - 1);
        if (endPage - startPage + 1 < maxButtons) {
            startPage = Math.max(1, endPage - maxButtons + 1);
        }

        for (let i = startPage; i <= endPage; i++) {
            const pageBtn = document.createElement('button');
            pageBtn.className = `join-item btn btn-sm ${i === this.state.currentItemsPage ? 'btn-active' : ''}`;
            pageBtn.textContent = i;
            pageBtn.addEventListener('click', () => {
                this.state.currentItemsPage = i;
                this.renderBrokenItemsTable();
            });
            pagination.appendChild(pageBtn);
        }

        const nextBtn = document.createElement('button');
        nextBtn.className = `join-item btn btn-sm ${this.state.currentItemsPage === totalPages ? 'btn-disabled' : ''}`;
        nextBtn.innerHTML = '<i class="bi bi-chevron-right"></i>';
        nextBtn.disabled = this.state.currentItemsPage === totalPages;
        if (this.state.currentItemsPage < totalPages) {
            nextBtn.addEventListener('click', () => {
                this.state.currentItemsPage++;
                this.renderBrokenItemsTable();
            });
        }
        pagination.appendChild(nextBtn);

        this.refs.itemsPagination.appendChild(pagination);
    }

    updateItemsStats() {
        this.refs.totalItemsCount.textContent = this.state.allBrokenItems.length;
        this.refs.modalFooterStats.textContent =
            `Total: ${this.state.allBrokenItems.length} | Filtered: ${this.state.filteredItems.length}`;
    }

    async processJob(jobId) {
        try {
            const response = await window.decypharrUtils.fetcher(`/api/repair/jobs/${jobId}/process`, {
                method: 'POST'
            });

            if (!response.ok) {
                const errorText = await response.text();
                throw new Error(errorText || 'Failed to process job');
            }

            window.decypharrUtils.createToast('Repair execution started', 'success');
            await this.loadJobs();
        } catch (error) {
            console.error('Error processing job:', error);
            window.decypharrUtils.createToast(`Error processing job: ${error.message}`, 'error');
        }
    }

    async stopJob(jobId) {
        if (!confirm('Are you sure you want to stop this run?')) return;

        try {
            const response = await window.decypharrUtils.fetcher(`/api/repair/jobs/${jobId}/stop`, {
                method: 'POST'
            });

            if (!response.ok) {
                const errorText = await response.text();
                throw new Error(errorText || 'Failed to stop run');
            }

            window.decypharrUtils.createToast('Stop requested', 'success');
            await this.loadJobs();
        } catch (error) {
            console.error('Error stopping job:', error);
            window.decypharrUtils.createToast(`Error stopping job: ${error.message}`, 'error');
        }
    }

    async deleteJob(jobId) {
        if (!confirm('Are you sure you want to delete this run?')) return;

        try {
            const response = await window.decypharrUtils.fetcher('/api/repair/jobs', {
                method: 'DELETE',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ ids: [jobId] })
            });

            if (!response.ok) {
                const errorText = await response.text();
                throw new Error(errorText || 'Failed to delete run');
            }

            window.decypharrUtils.createToast('Run deleted', 'success');
            await this.loadJobs();
        } catch (error) {
            console.error('Error deleting job:', error);
            window.decypharrUtils.createToast(`Error deleting job: ${error.message}`, 'error');
        }
    }

    async deleteSelectedJobs() {
        const selectedIds = Array.from(document.querySelectorAll('.job-checkbox:checked')).map((checkbox) => checkbox.value);
        if (selectedIds.length === 0) return;

        if (!confirm(`Are you sure you want to delete ${selectedIds.length} run(s)?`)) return;

        try {
            const response = await window.decypharrUtils.fetcher('/api/repair/jobs', {
                method: 'DELETE',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ ids: selectedIds })
            });

            if (!response.ok) {
                const errorText = await response.text();
                throw new Error(errorText || 'Failed to delete runs');
            }

            window.decypharrUtils.createToast(`${selectedIds.length} run(s) deleted`, 'success');
            await this.loadJobs();
        } catch (error) {
            console.error('Error deleting jobs:', error);
            window.decypharrUtils.createToast(`Error deleting jobs: ${error.message}`, 'error');
        }
    }

    toggleSelectAllJobs(checked) {
        const checkboxes = document.querySelectorAll('.job-checkbox:not(:disabled)');
        checkboxes.forEach((checkbox) => {
            checkbox.checked = checked;
        });
        this.updateJobSelectionState();
    }

    updateJobSelectionState() {
        const checkedBoxes = document.querySelectorAll('.job-checkbox:checked');
        const enabledBoxes = document.querySelectorAll('.job-checkbox:not(:disabled)');

        this.refs.deleteSelectedJobs.disabled = checkedBoxes.length === 0;

        if (enabledBoxes.length === 0) {
            this.refs.selectAllJobs.checked = false;
            this.refs.selectAllJobs.indeterminate = false;
        } else if (checkedBoxes.length === enabledBoxes.length) {
            this.refs.selectAllJobs.checked = true;
            this.refs.selectAllJobs.indeterminate = false;
        } else if (checkedBoxes.length > 0) {
            this.refs.selectAllJobs.checked = false;
            this.refs.selectAllJobs.indeterminate = true;
        } else {
            this.refs.selectAllJobs.checked = false;
            this.refs.selectAllJobs.indeterminate = false;
        }
    }

    async processCurrentJob() {
        if (!this.state.currentJob) return;
        await this.processJob(this.state.currentJob.id);
        this.refs.jobDetailsModal.close();
    }

    async stopCurrentJob() {
        if (!this.state.currentJob) return;
        await this.stopJob(this.state.currentJob.id);
        this.refs.jobDetailsModal.close();
    }

    handleItemTableClick(_e) {
        // kept for parity/future interaction; no-op for now
    }

    startAutoRefresh() {
        this.refreshInterval = setInterval(() => {
            const hasActiveJobs = this.state.jobs.some((job) => ['started', 'processing'].includes(job.status));
            if (hasActiveJobs || !this.refs.jobDetailsModal?.open) {
                this.loadJobs();
            }
        }, 10000);

        document.addEventListener('visibilitychange', () => {
            if (document.hidden) {
                if (this.refreshInterval) {
                    clearInterval(this.refreshInterval);
                    this.refreshInterval = null;
                }
            } else if (!this.refreshInterval) {
                this.startAutoRefresh();
            }
        });

        window.addEventListener('beforeunload', () => {
            if (this.refreshInterval) {
                clearInterval(this.refreshInterval);
            }
        });
    }

    formatDate(value) {
        if (!value) return 'N/A';
        const d = new Date(value);
        if (Number.isNaN(d.getTime())) return 'N/A';
        return d.toLocaleString();
    }
}

const RepairUtils = {
    formatRepairStatus(status, error = null) {
        const map = {
            pending: { icon: 'bi-clock', class: 'text-warning', message: 'Waiting to run' },
            started: { icon: 'bi-play-circle', class: 'text-primary', message: 'Repair in progress' },
            processing: { icon: 'bi-gear', class: 'text-info', message: 'Executing actions' },
            completed: { icon: 'bi-check-circle', class: 'text-success', message: 'Repair completed successfully' },
            failed: { icon: 'bi-x-circle', class: 'text-error', message: error || 'Repair failed' },
            cancelled: { icon: 'bi-stop-circle', class: 'text-warning', message: 'Repair was cancelled' }
        };
        return map[status] || { icon: 'bi-question-circle', class: 'text-gray-500', message: `Unknown status: ${status}` };
    }
};
