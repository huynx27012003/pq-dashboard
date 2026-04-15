import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MatCardModule } from '@angular/material/card';

@Component({
  selector: 'app-admin',
  standalone: true,
  imports: [CommonModule, MatCardModule],
  template: `
    <div class="admin-container p-6 w-full h-full">
      <h1 class="text-2xl font-semibold mb-6">Admin Dashboard</h1>
      <mat-card class="stats-card">
        <mat-card-header>
          <mat-card-title>System Overview</mat-card-title>
          <mat-card-subtitle>Exclusive Admin View</mat-card-subtitle>
        </mat-card-header>
        <mat-card-content class="pt-4">
          <p>Welcome to the Admin page. Only users with the <code>admin</code> role can access this page.</p>
        </mat-card-content>
      </mat-card>
    </div>
  `,
  styles: [`
    .admin-container {
      background-color: var(--mat-sys-surface-container);
    }
    .stats-card {
      max-width: 600px;
    }
  `]
})
export class AdminComponent {}
