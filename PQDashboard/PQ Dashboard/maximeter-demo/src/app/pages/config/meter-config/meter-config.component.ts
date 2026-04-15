import { Component, OnInit, OnDestroy, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule, ReactiveFormsModule, FormBuilder, FormGroup, Validators } from '@angular/forms';
import { MatCardModule } from '@angular/material/card';
import { MatIconModule } from '@angular/material/icon';
import { MatButtonModule } from '@angular/material/button';
import { MatTableModule } from '@angular/material/table';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatDialogModule, MatDialog } from '@angular/material/dialog';
import { MatSnackBar, MatSnackBarModule } from '@angular/material/snack-bar';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatChipsModule } from '@angular/material/chips';
import { MatSelectModule } from '@angular/material/select';
import { Subscription } from 'rxjs';
import { MeterApiService } from '../../../services/meter-api.service';
import {
  MeterLoopStatusService,
  MeterLoopStatus,
  mapRawStatusToConnection,
} from '../../../services/meter-loop-status.service';
import {
  ApiMeterInfo,
  MeterLoopStreamEvent,
  StartLoopStreamStatusData,
  LoopMeterStatusData,
  StopLoopResponse,
  EnergyRole,
  EnergySource,
} from '../../../interfaces/api.interface';

@Component({
  selector: 'app-meter-config',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    ReactiveFormsModule,
    MatCardModule,
    MatIconModule,
    MatButtonModule,
    MatTableModule,
    MatFormFieldModule,
    MatInputModule,
    MatDialogModule,
    MatSnackBarModule,
    MatProgressSpinnerModule,
    MatCheckboxModule,
    MatTooltipModule,
    MatChipsModule,
    MatSelectModule,
  ],
  templateUrl: './meter-config.component.html',
  styleUrl: './meter-config.component.scss',
})
export class MeterConfigComponent implements OnInit, OnDestroy {
  private meterApi = inject(MeterApiService);
  private fb = inject(FormBuilder);
  private snackBar = inject(MatSnackBar);
  private loopStatusService = inject(MeterLoopStatusService);
  private lastEventIdSent: string | null = null;
  private readonly storageKeys = {
    taskId: 'meterLoopTaskId',
    lastEventId: 'meterLoopLastEventId',
    meterIds: 'meterLoopMeterIds',
    running: 'meterLoopRunning',
  };

  meters: ApiMeterInfo[] = [];
  isLoading = false;
  isLoopRunning = false;
  currentTaskId: string | null = null;
  loopStatus: MeterLoopStatus = {
    isRunning: false,
    availableIds: [],
    functionalIds: [],
    meterStatusById: {},
    taskId: null,
    lastSlotTs: null,
  };
  private streamSubscription: Subscription | null = null;

  roles: EnergyRole[] = [];
  sources: EnergySource[] = [];

  displayedColumns: string[] = ['meter_id', 'serial_number', 'username', 'password', 'status', 'actions'];

  // Add/Edit form
  meterForm: FormGroup;
  isEditing = false;
  showForm = false;
  editingMeterId: number | null = null;

  constructor() {
    this.meterForm = this.fb.group({
      serial_number: ['', [Validators.required, Validators.pattern(/^\d+$/)]],
      username: ['', Validators.required],
      password: ['', Validators.required],
      meter_name: [''],
      outstation: ['', [Validators.pattern(/^\d+$/)]],
      type: ['', Validators.required],
      model: ['', Validators.required],
      survey_type: [''],
      owner_id: [1, Validators.required],
      role: [''],
      source_id: [{ value: '', disabled: true }],
    });
  }

  ngOnInit(): void {
    this.loadMeters();
    this.loadRoles();
    this.loadSources();
    this.loopStatusService.status$.subscribe(status => {
      this.loopStatus = status;
      this.isLoopRunning = status.isRunning;
    });
    this.resumeStreamIfNeeded();

    this.meterForm.get('role')?.valueChanges.subscribe(roleId => {
      const sourceControl = this.meterForm.get('source_id');
      const selectedRole = this.roles.find(r => r.id === roleId);
      if (selectedRole?.name === 'SOURCE') {
        sourceControl?.enable();
      } else {
        sourceControl?.disable();
        sourceControl?.setValue('');
      }
    });
  }

  ngOnDestroy(): void {
    this.stopStream();
  }

  loadMeters(): void {
    this.isLoading = true;
    this.meterApi.getAllMetersInfo().subscribe({
      next: (meters) => {
        this.meters = meters;
        this.isLoading = false;
      },
      error: (err) => {
        this.snackBar.open(`Error loading meters: ${err.message}`, 'Close', { duration: 5000 });
        this.isLoading = false;
      },
    });
  }

  loadRoles(): void {
    this.meterApi.getRoles().subscribe({
      next: (res) => {
        this.roles = res.items;
      },
      error: (err) => {
        this.snackBar.open(`Error loading roles: ${err.message}`, 'Close', { duration: 5000 });
      },
    });
  }

  loadSources(): void {
    this.meterApi.getSources().subscribe({
      next: (res) => {
        this.sources = res.items;
      },
      error: (err) => {
        this.snackBar.open(`Error loading sources: ${err.message}`, 'Close', { duration: 5000 });
      },
    });
  }

  // Reading Loop Control
  startReadingLoop(): void {
    if (this.meters.length === 0) {
      this.snackBar.open('No meters available to start loop', 'Close', { duration: 3000 });
      return;
    }

    const meterIds = this.meters.map(m => m.meter_id);
    this.startStream(meterIds, true);
  }

  stopReadingLoop(): void {
    this.stopStream();
    this.meterApi.stopReadingLoop().subscribe({
      next: (response: StopLoopResponse) => {
        this.isLoopRunning = false;
        this.currentTaskId = null;
        this.showLoopMessages([], [], 'stop');
        this.loopStatusService.reset();
        this.clearStoredLoopState();
      },
      error: (err) => {
        this.snackBar.open(`Error stopping loop: ${err.message}`, 'Close', { duration: 5000 });
      },
    });
  }

  // CRUD Operations
  showAddForm(): void {
    this.isEditing = false;
    this.showForm = true;
    this.editingMeterId = null;
    this.meterForm.reset({
      serial_number: '',
      username: '',
      password: '',
      meter_name: '',
      outstation: '',
      type: '',
      model: '',
      survey_type: '',
      owner_id: 1,
      role: '',
      source_id: '',
    });
  }

  showEditForm(meter: ApiMeterInfo): void {
    this.isEditing = true;
    this.showForm = true;
    this.editingMeterId = meter.meter_id;
    this.meterForm.patchValue({
      serial_number: meter.serial_number.toString(),
      username: meter.username,
      password: meter.password,
      meter_name: meter.meter_name || '',
      outstation: meter.outstation !== undefined ? meter.outstation.toString() : '',
      type: meter.type || '',
      model: meter.model || '',
      survey_type: (meter.survey_type || []).join(', '),
      owner_id: 1,
      role: meter.role || '',
      source_id: meter.source_id || '',
    });
  }

  cancelForm(): void {
    this.showForm = false;
    this.isEditing = false;
    this.editingMeterId = null;
    this.meterForm.reset({ owner_id: 1 });
  }

  saveMeter(): void {
    if (this.meterForm.invalid) {
      return;
    }

    const formData = {
      serial_number: Number(this.meterForm.value.serial_number),
      username: this.meterForm.value.username,
      password: this.meterForm.value.password,
      meter_name: this.meterForm.value.meter_name || undefined,
      outstation: this.meterForm.value.outstation ? Number(this.meterForm.value.outstation) : undefined,
      type: this.meterForm.value.type,
      model: this.meterForm.value.model,
      survey_type: this.parseSurveyType(this.meterForm.value.survey_type),
      owner_id: this.meterForm.value.owner_id,
      role: this.meterForm.value.role || undefined,
      source_id: this.meterForm.value.source_id ? Number(this.meterForm.value.source_id) : undefined,
    };

    if (this.isEditing) {
      this.meterApi.updateMeter(formData).subscribe({
        next: (response) => {
          this.snackBar.open(response.status, 'Close', { duration: 3000 });
          this.cancelForm();
          this.loadMeters();
        },
        error: (err) => {
          this.snackBar.open(`Error updating meter: ${err.message}`, 'Close', { duration: 5000 });
        },
      });
    } else {
      this.meterApi.addMeter(formData).subscribe({
        next: (response) => {
          this.snackBar.open(response.status, 'Close', { duration: 3000 });
          this.cancelForm();
          this.loadMeters();
        },
        error: (err) => {
          this.snackBar.open(`Error adding meter: ${err.message}`, 'Close', { duration: 5000 });
        },
      });
    }
  }

  deleteMeter(meter: ApiMeterInfo): void {
    if (!confirm(`Are you sure you want to delete meter ${meter.serial_number}?`)) {
      return;
    }

    this.meterApi.deleteMeter(meter.meter_id).subscribe({
      next: (response) => {
        this.snackBar.open(response.status, 'Close', { duration: 3000 });
        this.loadMeters();
      },
      error: (err) => {
        this.snackBar.open(`Error deleting meter: ${err.message}`, 'Close', { duration: 5000 });
      },
    });
  }

  private parseSurveyType(value: string | null | undefined): string[] | undefined {
    if (!value) return undefined;
    const items = value
      .split(',')
      .map(item => item.trim())
      .filter(Boolean);
    return items.length > 0 ? items : undefined;
  }

  getMeterStatus(meterId: number): { label: string; className: string } {
    const entry = this.loopStatus.meterStatusById[meterId];
    const connection = mapRawStatusToConnection(entry?.status, this.loopStatus.isRunning);
    if (connection === 'connected') {
      return { label: 'Connected', className: 'status-connected' };
    }
    if (connection === 'failed') {
      return { label: 'Failed', className: 'status-failed' };
    }
    return { label: 'Not Connected', className: 'status-not-connected' };
  }

  private showLoopMessages(availableIds: number[] | null | undefined, functionalIds: number[] | null | undefined, action: 'start' | 'stop'): void {
    const safeAvailableIds = availableIds || [];
    const safeFunctionalIds = functionalIds || [];

    if (!safeAvailableIds.length) {
      this.snackBar.open(action === 'start' ? 'No meters available' : 'No meters to stop', 'Close', { duration: 4000 });
      return;
    }

    const meterMap = new Map(this.meters.map(m => [m.meter_id, m]));
    const lines = safeAvailableIds.map(id => {
      const meter = meterMap.get(id);
      const name = meter?.meter_name || `Meter ${id}`;
      const serial = meter?.serial_number?.toString() || String(id);
      if (action === 'start') {
        return safeFunctionalIds.includes(id)
          ? `${name} ${serial} start read successfully`
          : `${name} ${serial} not responding`;
      }
      return `stop reading ${name} ${serial} success`;
    });

    this.snackBar.open(lines.join('\n'), 'Close', {
      duration: 6000,
      panelClass: ['multiline-snackbar'],
    });
  }

  private stopStream(): void {
    if (this.streamSubscription) {
      this.streamSubscription.unsubscribe();
      this.streamSubscription = null;
    }
  }

  private startStream(meterIds: number[], useResume: boolean): void {
    this.stopStream();
    const lastEventId = this.getStoredLastEventId(useResume);
    this.lastEventIdSent = lastEventId ?? null;

    this.streamSubscription = this.meterApi.startReadingLoopStream(meterIds, lastEventId).subscribe({
      next: (event: MeterLoopStreamEvent) => {
        if (event.id) {
          localStorage.setItem(this.storageKeys.lastEventId, event.id);
        }

        if (event.event === 'status') {
          const payload = event.data as StartLoopStreamStatusData;
          const storedTaskId = localStorage.getItem(this.storageKeys.taskId);
          if (this.lastEventIdSent && storedTaskId && payload.task_id && payload.task_id !== storedTaskId && payload.status === 'started') {
            this.clearStoredResume();
            this.startStream(meterIds, false);
            return;
          }

          this.isLoopRunning = true;
          this.currentTaskId = payload.task_id;
          this.loopStatusService.setFromStartEvent(payload);
          this.storeLoopState(payload.task_id, meterIds);
          this.showLoopMessages(payload.available_ids, payload.functional_ids, 'start');
          return;
        }

        if (event.event === 'meter_status') {
          const payload = event.data as LoopMeterStatusData;
          this.loopStatusService.updateMeterStatus(payload);
        }
      },
      error: (err) => {
        this.snackBar.open(`Error starting loop: ${err.message || err}`, 'Close', { duration: 5000 });
      },
    });
  }

  private resumeStreamIfNeeded(): void {
    const storedRunning = localStorage.getItem(this.storageKeys.running) === 'true';
    const storedIds = this.getStoredMeterIds();
    if (!storedRunning || storedIds.length === 0) {
      return;
    }
    this.startStream(storedIds, true);
  }

  private getStoredLastEventId(useResume: boolean): string | undefined {
    if (!useResume) {
      return undefined;
    }
    const storedTaskId = localStorage.getItem(this.storageKeys.taskId);
    const storedEventId = localStorage.getItem(this.storageKeys.lastEventId);
    if (!storedTaskId || !storedEventId) {
      return undefined;
    }
    return storedEventId;
  }

  private getStoredMeterIds(): number[] {
    const raw = localStorage.getItem(this.storageKeys.meterIds);
    if (!raw) {
      return [];
    }
    try {
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed)) {
        return parsed.map(value => Number(value)).filter(value => Number.isFinite(value));
      }
    } catch {
      return [];
    }
    return [];
  }

  private storeLoopState(taskId: string | null, meterIds: number[]): void {
    if (taskId) {
      localStorage.setItem(this.storageKeys.taskId, taskId);
    }
    localStorage.setItem(this.storageKeys.running, 'true');
    localStorage.setItem(this.storageKeys.meterIds, JSON.stringify(meterIds));
  }

  private clearStoredResume(): void {
    localStorage.removeItem(this.storageKeys.lastEventId);
  }

  private clearStoredLoopState(): void {
    localStorage.removeItem(this.storageKeys.taskId);
    localStorage.removeItem(this.storageKeys.lastEventId);
    localStorage.removeItem(this.storageKeys.meterIds);
    localStorage.removeItem(this.storageKeys.running);
  }
}
