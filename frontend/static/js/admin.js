/**
 * GeoBusca Admin & Management JS
 * Handles real-time alerts and management dashboard utilities.
 */

const AdminDashboard = {
    /**
     * Shows a toast notification for SLA breaches
     */
    showSLABreachAlert: function(count) {
        if (count <= 0) return;
        
        const toastHTML = `
            <div class="toast-container position-fixed bottom-0 end-0 p-3">
                <div id="slaToast" class="toast border-0 shadow-lg" role="alert" aria-live="assertive" aria-atomic="true">
                    <div class="toast-header bg-danger text-white rounded-top">
                        <i class="bi bi-exclamation-triangle-fill me-2"></i>
                        <strong class="me-auto">Alerta de SLA</strong>
                        <button type="button" class="btn-close btn-close-white" data-bs-dismiss="toast" aria-label="Close"></button>
                    </div>
                    <div class="toast-body bg-white rounded-bottom">
                        Se han detectado <strong>${count}</strong> visitas con SLA incumplido hoy.
                        <div class="mt-2 pt-2 border-top">
                            <a href="/admin/manager-dashboard" class="btn btn-danger btn-sm w-100">Ver Detalles</a>
                        </div>
                    </div>
                </div>
            </div>
        `;
        
        let container = document.getElementById('admin-toasts');
        if (!container) {
            container = document.createElement('div');
            container.id = 'admin-toasts';
            document.body.appendChild(container);
        }
        container.innerHTML = toastHTML;
        
        const toastElement = document.getElementById('slaToast');
        const toast = new bootstrap.Toast(toastElement, { delay: 10000 });
        toast.show();
    }
};

// Auto-check for alerts if we are on a management page
document.addEventListener('DOMContentLoaded', function() {
    // This could be expanded to pull from an API periodically
    console.log('Admin module loaded');
});
